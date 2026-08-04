import json
import logging
import time

import websocket

from ...api_key import get_api_key


WS_URL = "wss://api.datamule.xyz/v3/websocket"
logger = logging.getLogger(__name__)


class WebSocketIdleTimeout(TimeoutError):
    """Raised when the stream receives no application messages for too long."""


class WebSocketPingTimeout(TimeoutError):
    """Raised when the stream does not receive a pong for its last ping."""


class WebSocketServerError(Exception):
    """Raised when the websocket sends an application-level error event."""


def stream_filings(
    api_key=None,
    idle_timeout=300,
    ping_interval=60,
    ping_timeout=10,
    recv_timeout=30,
    reconnect_delay=1,
):
    """
    Connect to the SEC filings websocket and yield live filing objects.

    Reconnects automatically when the socket closes, transport errors occur,
    pings stop receiving pongs, or no filing/heartbeat messages arrive before
    `idle_timeout`.
    """
    _validate_timeouts(
        idle_timeout=idle_timeout,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        recv_timeout=recv_timeout,
        reconnect_delay=reconnect_delay,
    )
    key = get_api_key(api_key)

    while True:
        try:
            yield from _stream_once(
                key,
                idle_timeout=idle_timeout,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                recv_timeout=recv_timeout,
            )
            logger.info("WebSocket closed, reconnecting")
        except websocket.WebSocketBadStatusException as exc:
            if exc.status_code in (401, 403):
                logger.exception("WebSocket authentication failed")
                raise
            logger.exception("WebSocket stream failed, reconnecting")
        except (
            websocket.WebSocketException,
            WebSocketIdleTimeout,
            WebSocketPingTimeout,
            OSError,
        ):
            logger.exception("WebSocket stream failed, reconnecting")

        if reconnect_delay:
            time.sleep(reconnect_delay)


def _stream_once(key, idle_timeout, ping_interval, ping_timeout, recv_timeout):
    socket_timeout = _socket_timeout(recv_timeout, ping_timeout, ping_interval)
    ws = websocket.create_connection(
        WS_URL,
        timeout=socket_timeout,
        header=[
            f"Authorization: Bearer {key}",
            "User-Agent: datamule-hub",
        ],
    )
    logger.info("WebSocket connected")

    last_activity = time.monotonic()
    awaiting_pong = None
    pong_deadline = None
    next_ping_at = _next_ping_at(ping_interval)

    try:
        while True:
            now = time.monotonic()
            if idle_timeout and now - last_activity > idle_timeout:
                raise WebSocketIdleTimeout(
                    f"No messages received for {idle_timeout}s"
                )

            if awaiting_pong and now > pong_deadline:
                raise WebSocketPingTimeout(
                    f"No pong received within {ping_timeout}s"
                )

            if next_ping_at and awaiting_pong is None and now >= next_ping_at:
                awaiting_pong, pong_deadline = _send_ping(ws, ping_timeout)
                next_ping_at = _next_ping_at(ping_interval)

            try:
                opcode, raw = ws.recv_data(control_frame=True)
            except websocket.WebSocketTimeoutException:
                continue

            if opcode == websocket.ABNF.OPCODE_CLOSE:
                return

            if opcode == websocket.ABNF.OPCODE_PONG:
                if awaiting_pong is None or raw == awaiting_pong:
                    awaiting_pong = None
                    pong_deadline = None
                continue

            if opcode == websocket.ABNF.OPCODE_PING:
                continue

            if opcode not in (
                websocket.ABNF.OPCODE_TEXT,
                websocket.ABNF.OPCODE_BINARY,
            ):
                continue

            last_activity = time.monotonic()
            event = json.loads(raw)
            if event.get("type") == "error":
                raise WebSocketServerError(event.get("error", "WebSocket error"))

            if event.get("type") != "filing":
                continue

            filing = event.get("item")
            if filing is None:
                continue

            logger.info("WebSocket emitted filing")
            yield filing
    finally:
        ws.close()


def _send_ping(ws, ping_timeout):
    payload = str(time.monotonic()).encode("ascii")
    ws.ping(payload)
    return payload, time.monotonic() + ping_timeout


def _next_ping_at(ping_interval):
    if not ping_interval or ping_interval <= 0:
        return None
    return time.monotonic() + ping_interval


def _socket_timeout(recv_timeout, ping_timeout, ping_interval):
    timeouts = [timeout for timeout in (recv_timeout,) if timeout and timeout > 0]
    if ping_interval and ping_interval > 0:
        timeouts.append(ping_timeout)
    return min(timeouts) if timeouts else None


def _validate_timeouts(
    idle_timeout,
    ping_interval,
    ping_timeout,
    recv_timeout,
    reconnect_delay,
):
    values = {
        "idle_timeout": idle_timeout,
        "ping_interval": ping_interval,
        "ping_timeout": ping_timeout,
        "recv_timeout": recv_timeout,
        "reconnect_delay": reconnect_delay,
    }
    for name, value in values.items():
        if value is not None and value < 0:
            raise ValueError(f"{name} must be non-negative or None.")

    if ping_interval and ping_interval > 0:
        if ping_timeout is None or ping_timeout <= 0:
            raise ValueError("ping_timeout must be positive when pings are enabled.")

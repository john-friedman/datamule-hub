import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime

import websocket

from ...api_key import get_api_key


WS_URL = "wss://api.datamule.xyz/v3/websocket"
FASTEST_TOKEN_URL = (
    "https://api.datamule.xyz/v3/auth/service-token"
    "?service=fastest-sec-filings-websocket"
)
logger = logging.getLogger(__name__)


class WebSocketIdleTimeout(TimeoutError):
    """Raised when the stream receives no application messages for too long."""


class WebSocketPingTimeout(TimeoutError):
    """Raised when the stream does not receive a pong for its last ping."""


class WebSocketServerError(Exception):
    """Raised when the websocket sends an application-level error event."""


class FastestFilingsTokenError(Exception):
    """Raised when paid fastest-filings access cannot be issued."""


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


def stream_fastest_filings(
    api_key=None,
    idle_timeout=None,
    ping_interval=60,
    ping_timeout=10,
    recv_timeout=30,
    reconnect_delay=1,
):
    """
    Purchase access and yield events from the fastest SEC filings websocket.

    Issuing a service token charges $0.25. Transport reconnects reuse the same
    token; a new token is requested only after the current token expires.
    """
    _validate_timeouts(
        idle_timeout=idle_timeout,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        recv_timeout=recv_timeout,
        reconnect_delay=reconnect_delay,
    )
    key = get_api_key(api_key)
    access = _request_fastest_access(key)

    while True:
        if _fastest_access_expired(access):
            access = _request_fastest_access(key)

        try:
            yield from _stream_fastest_once(
                access["websocket_url"],
                access["token"],
                idle_timeout=idle_timeout,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                recv_timeout=recv_timeout,
            )
            logger.info("Fastest filings websocket closed, reconnecting")
        except websocket.WebSocketBadStatusException as exc:
            if exc.status_code in (401, 403):
                if _fastest_access_expired(access, skew_seconds=5):
                    access = _request_fastest_access(key)
                    continue
                logger.exception("Fastest filings websocket authentication failed")
                raise
            logger.exception("Fastest filings websocket failed, reconnecting")
        except (
            websocket.WebSocketException,
            WebSocketIdleTimeout,
            WebSocketPingTimeout,
            OSError,
        ):
            logger.exception("Fastest filings websocket failed, reconnecting")

        if reconnect_delay:
            time.sleep(reconnect_delay)


def _request_fastest_access(key):
    request = urllib.request.Request(
        FASTEST_TOKEN_URL,
        data=b"",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {key}",
            "User-Agent": "datamule-hub",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            message = json.loads(body).get("error", body)
        except json.JSONDecodeError:
            message = body
        raise FastestFilingsTokenError(
            f"Fastest filings access request failed ({exc.code}): {message}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise FastestFilingsTokenError(
            f"Fastest filings access request failed: {exc}"
        ) from exc

    if not payload.get("success"):
        raise FastestFilingsTokenError(
            f"Fastest filings access request failed: {payload.get('error')}"
        )

    data = payload.get("data") or {}
    token = data.get("token")
    websocket_url = data.get("websocket_url")
    expires_at = data.get("expires_at")
    if not token or not websocket_url or not expires_at:
        raise FastestFilingsTokenError(
            "Fastest filings access response is missing token details."
        )

    try:
        expires_timestamp = datetime.fromisoformat(
            expires_at.replace("Z", "+00:00")
        ).timestamp()
    except (AttributeError, TypeError, ValueError) as exc:
        raise FastestFilingsTokenError(
            "Fastest filings access response has an invalid expiration."
        ) from exc

    billing = (payload.get("metadata") or {}).get("billing") or {}
    charge = billing.get("total_charge")
    remaining_balance = billing.get("remaining_balance")
    if charge is not None and remaining_balance is not None:
        logger.info(
            "Fastest filings access issued: cost=$%.2f, remaining balance=$%.2f, expires=%s",
            float(charge),
            float(remaining_balance),
            expires_at,
        )
    else:
        logger.info("Fastest filings access issued until %s", expires_at)

    return {
        "token": token,
        "websocket_url": websocket_url,
        "expires_at": expires_at,
        "expires_timestamp": expires_timestamp,
        "billing": billing,
    }


def _fastest_access_expired(access, skew_seconds=0):
    return time.time() + skew_seconds >= access["expires_timestamp"]


def _stream_once(key, idle_timeout, ping_interval, ping_timeout, recv_timeout):
    yield from _stream_url_once(
        WS_URL,
        key,
        _standard_filing_from_event,
        idle_timeout,
        ping_interval,
        ping_timeout,
        recv_timeout,
    )


def _stream_fastest_once(
    websocket_url,
    token,
    idle_timeout,
    ping_interval,
    ping_timeout,
    recv_timeout,
):
    yield from _stream_url_once(
        websocket_url,
        token,
        _fastest_filing_from_event,
        idle_timeout,
        ping_interval,
        ping_timeout,
        recv_timeout,
    )


def _stream_url_once(
    websocket_url,
    credential,
    filing_from_event,
    idle_timeout,
    ping_interval,
    ping_timeout,
    recv_timeout,
):
    socket_timeout = _socket_timeout(recv_timeout, ping_timeout, ping_interval)
    ws = websocket.create_connection(
        websocket_url,
        timeout=socket_timeout,
        header=[
            f"Authorization: Bearer {credential}",
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
            filing = filing_from_event(event)
            if filing is None:
                continue

            logger.info("WebSocket emitted filing")
            yield filing
    finally:
        ws.close()


def _standard_filing_from_event(event):
    if not isinstance(event, dict):
        return None
    if event.get("type") == "error":
        raise WebSocketServerError(event.get("error", "WebSocket error"))
    if event.get("type") != "filing":
        return None
    return event.get("item")


def _fastest_filing_from_event(event):
    if not isinstance(event, dict):
        return None
    if event.get("type") == "error":
        raise WebSocketServerError(event.get("error", "WebSocket error"))
    if "accession" not in event:
        return None
    return event


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

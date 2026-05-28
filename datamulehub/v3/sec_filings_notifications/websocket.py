import json
import logging

import websocket

from ...api_key import api_key


WS_URL = "wss://api.datamule.xyz/v3/websocket"
logger = logging.getLogger(__name__)


def stream_filings():
    """
    Connect to the SEC filings websocket and yield live filing objects.
    """
    ws = websocket.create_connection(
        WS_URL,
        header=[
            f"Authorization: Bearer {api_key}",
            "User-Agent: datamule-hub",
        ],
    )
    logger.info("WebSocket connected")

    try:
        while True:
            raw = ws.recv()
            if raw is None:
                return

            event = json.loads(raw)
            if event.get("type") == "error":
                raise Exception(event.get("error", "WebSocket error"))

            if event.get("type") != "filing":
                continue

            filing = event.get("item")
            if filing is None:
                continue

            logger.info("WebSocket emitted filing")
            yield filing
    finally:
        ws.close()

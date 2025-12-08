import argparse
import asyncio
import json
from typing import Dict, Set

from aiohttp import web, WSMsgType


class Hub:
    def __init__(self) -> None:
        self.producers: Dict[str, web.WebSocketResponse] = {}
        self.consumers: Dict[str, Set[web.WebSocketResponse]] = {}
        self.lock = asyncio.Lock()

    async def register_producer(self, producer_id: str, ws: web.WebSocketResponse) -> bool:
        async with self.lock:
            if producer_id in self.producers:
                return False
            self.producers[producer_id] = ws
        return True

    async def unregister_producer(self, producer_id: str) -> None:
        async with self.lock:
            self.producers.pop(producer_id, None)
            consumers = self.consumers.pop(producer_id, set())
        for consumer in consumers:
            await consumer.close(code=1000, message=b"Producer disconnected")

    async def register_consumer(self, producer_id: str, ws: web.WebSocketResponse) -> None:
        async with self.lock:
            self.consumers.setdefault(producer_id, set()).add(ws)

    async def unregister_consumer(self, producer_id: str, ws: web.WebSocketResponse) -> None:
        async with self.lock:
            if producer_id in self.consumers:
                self.consumers[producer_id].discard(ws)
                if not self.consumers[producer_id]:
                    self.consumers.pop(producer_id, None)

    async def broadcast_frame(self, producer_id: str, data: bytes) -> None:
        async with self.lock:
            consumers = list(self.consumers.get(producer_id, []))
        for consumer in consumers:
            try:
                await consumer.send_bytes(data)
            except Exception:
                await self.unregister_consumer(producer_id, consumer)


async def producer_handler(request: web.Request) -> web.WebSocketResponse:
    hub: Hub = request.app["hub"]
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    producer_id = request.query.get("id")
    if not producer_id:
        await ws.close(code=4000, message=b"Missing producer id")
        return ws

    if not await hub.register_producer(producer_id, ws):
        await ws.close(code=4001, message=b"Producer already registered")
        return ws

    try:
        async for msg in ws:
            if msg.type == WSMsgType.BINARY:
                await hub.broadcast_frame(producer_id, msg.data)
            elif msg.type == WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("type") == "frame":
                    payload = data.get("payload")
                    if payload:
                        await hub.broadcast_frame(producer_id, payload.encode())
            elif msg.type == WSMsgType.CLOSE:
                break
    finally:
        await hub.unregister_producer(producer_id)

    return ws


async def consumer_handler(request: web.Request) -> web.WebSocketResponse:
    hub: Hub = request.app["hub"]
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    producer_id = request.query.get("id")
    if not producer_id:
        await ws.close(code=4000, message=b"Missing producer id")
        return ws

    await hub.register_consumer(producer_id, ws)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        await hub.unregister_consumer(producer_id, ws)

    return ws


def create_app() -> web.Application:
    hub = Hub()
    app = web.Application()
    app["hub"] = hub
    app.add_routes(
        [
            web.get("/ws/producer", producer_handler),
            web.get("/ws/consumer", consumer_handler),
        ]
    )
    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WebSocket hub for camera producers/consumers.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app()
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

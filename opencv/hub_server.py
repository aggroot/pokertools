import argparse
import asyncio
import json
from contextlib import suppress
from typing import Dict, Set, Tuple

from aiohttp import web, WSMsgType


class Hub:
    def __init__(self) -> None:
        self.producers: Dict[str, web.WebSocketResponse] = {}
        self.consumers: Dict[str, Set[web.WebSocketResponse]] = {}
        self.consumer_queues: Dict[Tuple[str, web.WebSocketResponse], asyncio.Queue[bytes]] = {}
        self.consumer_tasks: Dict[Tuple[str, web.WebSocketResponse], asyncio.Task[None]] = {}
        self.lock = asyncio.Lock()

    async def register_producer(self, producer_id: str, ws: web.WebSocketResponse) -> bool:
        async with self.lock:
            if producer_id in self.producers:
                return False
            self.producers[producer_id] = ws
            self.consumers.setdefault(producer_id, set())
        return True

    async def unregister_producer(self, producer_id: str) -> None:
        async with self.lock:
            ws = self.producers.pop(producer_id, None)
            consumers = list(self.consumers.pop(producer_id, set()))
        for consumer in consumers:
            await self.unregister_consumer(producer_id, consumer)
        if ws and not ws.closed:
            await ws.close()

    async def register_consumer(self, producer_id: str, ws: web.WebSocketResponse) -> bool:
        async with self.lock:
            if producer_id not in self.producers:
                return False
            self.consumers.setdefault(producer_id, set()).add(ws)
            queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=2)
            self.consumer_queues[(producer_id, ws)] = queue
            task = asyncio.create_task(self._forward_frames(producer_id, ws, queue))
            self.consumer_tasks[(producer_id, ws)] = task
        return True

    async def unregister_consumer(self, producer_id: str, ws: web.WebSocketResponse) -> None:
        async with self.lock:
            if producer_id in self.consumers:
                self.consumers[producer_id].discard(ws)
            queue = self.consumer_queues.pop((producer_id, ws), None)
            task = self.consumer_tasks.pop((producer_id, ws), None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if queue:
            while not queue.empty():
                queue.get_nowait()
        if not ws.closed:
            with suppress(Exception):
                await ws.close()

    async def broadcast_frame(self, producer_id: str, frame: bytes) -> None:
        async with self.lock:
            targets = [
                self.consumer_queues.get((producer_id, consumer))
                for consumer in self.consumers.get(producer_id, set())
            ]
        for queue in targets:
            if queue is None:
                continue
            if queue.full():
                with suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            queue.put_nowait(frame)

    async def _forward_frames(
        self, producer_id: str, consumer: web.WebSocketResponse, queue: asyncio.Queue[bytes]
    ) -> None:
        try:
            while True:
                frame = await queue.get()
                await consumer.send_bytes(frame)
        except Exception:
            asyncio.create_task(self.unregister_consumer(producer_id, consumer))


hub = Hub()


async def producer_handler(request: web.Request) -> web.WebSocketResponse:
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
                payload = json.loads(msg.data).get("payload")
                if payload:
                    await hub.broadcast_frame(producer_id, payload.encode())
            elif msg.type == WSMsgType.CLOSE:
                break
    finally:
        await hub.unregister_producer(producer_id)

    return ws


async def consumer_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    producer_id = request.query.get("id")
    if not producer_id:
        await ws.close(code=4000, message=b"Missing producer id")
        return ws

    if not await hub.register_consumer(producer_id, ws):
        await ws.close(code=4004, message=b"Producer not available")
        return ws

    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        await hub.unregister_consumer(producer_id, ws)

    return ws


def create_app() -> web.Application:
    app = web.Application()
    app.add_routes(
        [
            web.get("/ws/producer", producer_handler),
            web.get("/ws/consumer", consumer_handler),
        ]
    )
    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WebSocket frame hub.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    web.run_app(create_app(), host=args.host, port=args.port)


if __name__ == "__main__":
    main()

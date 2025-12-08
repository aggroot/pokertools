import argparse
import asyncio
from pathlib import Path

import aiohttp
import cv2
import numpy as np


async def consume(args: argparse.Namespace) -> None:
    window_name = f"Stream {args.producer_id}"

    async with aiohttp.ClientSession() as session:
        params = {"id": args.producer_id}
        async with session.ws_connect(args.server_url, params=params, heartbeat=30) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    data = msg.data
                    print(f"Received frame ({len(data)} bytes)")
                    if args.show or args.save_dir:
                        frame = decode_frame(data)
                        if frame is None:
                            continue
                        if args.show:
                            cv2.imshow(window_name, frame)
                            cv2.waitKey(1)
                        if args.save_dir:
                            save_frame(frame, Path(args.save_dir))
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
    if args.show:
        cv2.destroyWindow(window_name)


def decode_frame(data: bytes):
    array = np.frombuffer(data, dtype=np.uint8)
    return cv2.imdecode(array, cv2.IMREAD_COLOR)


def save_frame(frame, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    filepath = directory / f"{int(asyncio.get_event_loop().time() * 1000)}.jpg"
    cv2.imwrite(str(filepath), frame)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WebSocket consumer receiving frames.")
    parser.add_argument(
        "--server-url", default="ws://localhost:9000/ws/consumer", help="Go hub consumer endpoint."
    )
    parser.add_argument("--producer-id", required=True, help="Producer ID to subscribe to.")
    parser.add_argument(
        "--show", action="store_true", help="Display the stream using OpenCV as frames arrive."
    )
    parser.add_argument(
        "--save-dir",
        default=None,
        help="Directory to save received frames (disabled by default).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(consume(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

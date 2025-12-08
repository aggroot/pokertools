import argparse
import asyncio
from pathlib import Path

import cv2
import nats
import numpy as np


async def consume(args: argparse.Namespace) -> None:
    nc = await nats.connect(args.nats_url)
    subject = f"{args.subject_prefix}.{args.producer_id}"

    window_name = f"Stream {args.producer_id}"

    async def handle_message(msg):
        data = msg.data
        print(f"Received frame ({len(data)} bytes)")
        if args.show or args.save_dir:
            array = np.frombuffer(data, dtype=np.uint8)
            frame = cv2.imdecode(array, cv2.IMREAD_COLOR)
            if frame is None:
                return
            if args.show:
                cv2.imshow(window_name, frame)
                cv2.waitKey(1)
            if args.save_dir:
                ts_path = Path(args.save_dir) / f"{int(asyncio.get_event_loop().time() * 1000)}.jpg"
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                cv2.imwrite(str(ts_path), frame)

    await nc.subscribe(subject, cb=lambda msg: asyncio.create_task(handle_message(msg)))

    print(f"Subscribed to {subject}")
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        if args.show:
            cv2.destroyWindow(window_name)
        await nc.drain()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NATS consumer receiving camera frames.")
    parser.add_argument(
        "--nats-url", default="nats://127.0.0.1:4222", help="NATS server URL."
    )
    parser.add_argument(
        "--subject-prefix", default="cams", help="Subject prefix used when subscribing."
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

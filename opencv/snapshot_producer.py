import argparse
import asyncio
import threading
import time
from typing import Optional

import cv2
import nats


class FrameBuffer:
    def __init__(self, quality: int, fps: int, interval: float) -> None:
        self.quality = quality
        self.interval = interval if interval > 0 else (1.0 / fps if fps > 0 else 0)
        self._lock = threading.Lock()
        self._frame: Optional[bytes] = None

    def start_capture(self, device: str, width: int, height: int, fps: int) -> None:
        def _loop() -> None:
            cap = cv2.VideoCapture(device)
            if not cap.isOpened():
                raise RuntimeError(f"Unable to open camera: {device}")
            cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
            cap.set(cv2.CAP_PROP_FPS, fps)

            while True:
                ret, frame = cap.read()
                if not ret:
                    continue
                success, buffer = cv2.imencode(
                    ".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, self.quality]
                )
                if success:
                    with self._lock:
                        self._frame = buffer.tobytes()
                if self.interval > 0:
                    time.sleep(self.interval)

        thread = threading.Thread(target=_loop, daemon=True)
        thread.start()

    def get_frame(self) -> Optional[bytes]:
        with self._lock:
            return self._frame


async def run_producer(args: argparse.Namespace) -> None:
    buffer = FrameBuffer(quality=args.quality, fps=args.fps, interval=args.interval)
    buffer.start_capture(args.device, args.width, args.height, args.fps)

    nc = await nats.connect(args.nats_url)
    subject = f"{args.subject_prefix}.{args.producer_id}"

    try:
        while True:
            frame = buffer.get_frame()
            if frame is None:
                await asyncio.sleep(0.01)
                continue
            await nc.publish(subject, frame)
            if args.publish_interval > 0:
                await asyncio.sleep(args.publish_interval)
            else:
                await asyncio.sleep(0)
    finally:
        await nc.drain()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WebSocket producer pushing camera frames.")
    parser.add_argument("--device", default="/dev/video0", help="Video device path.")
    parser.add_argument("--width", type=int, default=1920, help="Capture width.")
    parser.add_argument("--height", type=int, default=1080, help="Capture height.")
    parser.add_argument("--fps", type=int, default=30, help="Capture FPS.")
    parser.add_argument("--quality", type=int, default=95, help="JPEG quality 0-100.")
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Seconds between frames (0 to match FPS).",
    )
    parser.add_argument(
        "--nats-url", default="nats://127.0.0.1:4222", help="NATS server URL."
    )
    parser.add_argument(
        "--subject-prefix", default="cams", help="Subject prefix used when publishing frames."
    )
    parser.add_argument("--producer-id", required=True, help="Unique producer ID.")
    parser.add_argument(
        "--publish-interval",
        type=float,
        default=0.0,
        help="Seconds to wait between publishes (0 publishes every frame).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_producer(args))


if __name__ == "__main__":
    main()

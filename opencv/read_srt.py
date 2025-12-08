import argparse
import subprocess

import cv2
import numpy as np


def main() -> None:
    parser = argparse.ArgumentParser(description="Read SRT stream via ffmpeg pipe")
    parser.add_argument("url", help="SRT URL, e.g. srt://:9000?mode=listener&latency=80")
    parser.add_argument("--width", type=int, default=1920)
    parser.add_argument("--height", type=int, default=1080)
    args = parser.parse_args()

    cmd = [
        "ffmpeg", "-loglevel", "quiet",
        "-i", args.url,
        "-pix_fmt", "bgr24", "-f", "rawvideo", "-an", "pipe:1"
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    frame_size = args.width * args.height * 3

    try:
        while True:
            data = proc.stdout.read(frame_size)
            if len(data) != frame_size:
                break
            frame = np.frombuffer(data, dtype=np.uint8).reshape((args.height, args.width, 3))
            cv2.imshow("SRT Stream", frame)
            if cv2.waitKey(1) == 27:
                break
    finally:
        proc.terminate()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    main()

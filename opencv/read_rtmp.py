import argparse
import cv2


def main() -> None:
    parser = argparse.ArgumentParser(description="Read RTMP stream with OpenCV")
    parser.add_argument("url", help="RTMP stream URL, e.g. rtmp://localhost/live/stream")
    parser.add_argument(
        "--display", action="store_true", help="Show the stream in a window"
    )
    parser.add_argument("--save", help="Optional path to save frames as video (MP4)")
    args = parser.parse_args()

    cap = cv2.VideoCapture(args.url)
    if not cap.isOpened():
        raise RuntimeError(f"Unable to open stream: {args.url}")

    writer = None
    if args.save:
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        writer = cv2.VideoWriter(args.save, fourcc, fps, (width, height))

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            if args.display:
                cv2.imshow("RTMP Stream", frame)
                if cv2.waitKey(1) & 0xFF == ord("q"):
                    break
            if writer is not None:
                writer.write(frame)
    finally:
        cap.release()
        if writer is not None:
            writer.release()
        if args.display:
            cv2.destroyAllWindows()


if __name__ == "__main__":
    main()

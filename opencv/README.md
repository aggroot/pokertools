# RTMP Streaming Setup

This workspace contains a simple nginx-rtmp server (via Docker) and utilities to publish and view webcam streams.

## Running the RTMP server

1. Ensure Docker is installed (Windows USB passthrough requires usbipd and port forwarding):
   ```bash
   usbipd bind --busid 1-2
   usbipd attach --wsl --busid 1-2
   # when finished:
   usbipd detach --busid 1-2
   ```
   Open firewall/port forwarding for WSL (example values):
   ```powershell
   netsh advfirewall firewall add rule `
     name="wsl2_9992" `
     dir=in `
     action=allow `
     protocol=TCP `
     localport=9992

   netsh interface portproxy add v4tov4 `
     listenaddress=192.168.100.101 `
     listenport=9992 `
     connectaddress=172.25.130.241 `
     connectport=9992
   ```
2. Start the RTMP server (example):
   ```bash
   docker run -d \\
     -p 9992:1935 \\
     --name nginx-rtmp \\
     tiangolo/nginx-rtmp
   ```
   *(Or use `docker compose` with your own config if you prefer.)*
3. The server listens on:
   - RTMP ingest/playback: `rtmp://<host>:9992/live/<stream_key>`

## Streaming the USB Camera with FFmpeg

Example command (includes audio and low-latency x264 settings):
```bash
ffmpeg \
  -f v4l2 -thread_queue_size 512 -framerate 30 -video_size 1920x1080 -i /dev/video0 \
  -f lavfi -i anullsrc=channel_layout=stereo:sample_rate=44100 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -b:v 4000k -maxrate 4000k -bufsize 16000k \
  -g 60 -keyint_min 60 -sc_threshold 0 -pix_fmt yuv420p \
  -c:a aac -b:a 128k -ar 44100 -ac 2 \
  -f flv -flvflags +no_duration_filesize \
  "rtmp://agglab.go.ro:9992/live/cam1"
```
Replace `/dev/video0` with your webcam device, and `cam1` with any stream key.

## Viewing the Stream

Use `read_rtmp.py` to view or record:
```bash
pip install opencv-python
python read_rtmp.py rtmp://agglab.go.ro:9992/live/cam1 --display
```
Add `--save output.mp4` to record locally.

## Reading an SRT Stream via FFmpeg Pipe (C++ example)

```c++
#include <opencv2/opencv.hpp>
#include <cstdio>

int main() {
    const int W = 1920;
    const int H = 1080;

    FILE* pipe = popen(
        "ffmpeg -loglevel quiet "
        "-i \"srt://:9000?mode=listener&latency=80\" "
        "-pix_fmt bgr24 -f rawvideo -an pipe:1",
        "r"
    );

    if (!pipe) return -1;

    cv::Mat frame(H, W, CV_8UC3);

    while (true) {
        size_t bytes = fread(frame.data, 1, W * H * 3, pipe);
        if (bytes != W * H * 3) break;

        cv::imshow("SRT via FFmpeg Pipe", frame);
        if (cv::waitKey(1) == 27) break;
    }
    pclose(pipe);
}
```

Compile with `pkg-config --cflags --libs opencv4`. Replace width/height and the SRT URL as needed.

## Reading an SRT Stream via FFmpeg Pipe (Python example)

```python
import subprocess
import cv2
import numpy as np

W, H = 1920, 1080
cmd = [
    "ffmpeg", "-loglevel", "quiet",
    "-i", "srt://:9000?mode=listener&latency=80",
    "-pix_fmt", "bgr24", "-f", "rawvideo", "-an", "pipe:1"
]

proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

while True:
    data = proc.stdout.read(W * H * 3)
    if len(data) != W * H * 3:
        break
    frame = np.frombuffer(data, dtype=np.uint8).reshape((H, W, 3))
    cv2.imshow("SRT via FFmpeg Pipe", frame)
    if cv2.waitKey(1) == 27:
        break

proc.terminate()
cv2.destroyAllWindows()
```

Install `opencv-python` and ensure `ffmpeg` is in `PATH`. Adjust size/URL as needed.

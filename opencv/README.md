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

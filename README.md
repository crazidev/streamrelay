# StreamRelay

Low-latency video streaming from phones and browsers directly to Python applications — delivered with zero-copy shared memory and virtual camera support.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

---

## Highlights

- **Zero-Install Web Client**: Stream phone or laptop cameras directly from any modern browser (iOS Safari, Android Chrome, desktop) over HTTPS.
- **Hardware-Accelerated Codecs**: H.264 WebCodecs and serialized JPEG modes with adaptive bitrate and resolution negotiation up to 4K.
- **Ultra-Low Latency**: Millisecond-level end-to-end delivery with synchronized client-server clock offset measurement.
- **Lock-Free Shared Memory**: Delivers BGR frames to external Python consumer processes using an atomic 12-byte header and shared memory buffer (`O(1)` access without sockets or pickling overhead).
- **Virtual Camera Output**: Stream directly into virtual camera inputs on macOS (OBS Virtual Camera / CoreMediaIO), Linux (`v4l2loopback`), and Windows.
- **Interactive Terminal Dashboard**: Rich terminal UI showing real-time wire bitrate, true stream FPS, latency, and live keyboard controls.
- **Dual Transports**: WebSocket for standard LAN streaming, plus SRT (Secure Reliable Transport via ffmpeg) for jitter-heavy or remote links.

---

## Installation

### From GitHub

```bash
pip install git+https://github.com/crazidev/streamrelay.git
```

### From Local Source

```bash
git clone https://github.com/crazidev/streamrelay.git
cd streamrelay
pip install -e .
```

### Optional Dependencies

StreamRelay installs core functionality by default. You can install optional bundles:

```bash
# Everything (PyAV H.264 decoder, rich CLI, pyvirtualcam, psutil)
pip install -e ".[all]"

# Or specific components:
pip install -e ".[h264]"     # PyAV for server-side H.264 decoding
pip install -e ".[virtual]"  # pyvirtualcam for virtual camera output
pip install -e ".[cli]"      # Rich interactive terminal UI
```

### System Prerequisites (Optional)

1. **Virtual Camera**:
   - **macOS**: Install [OBS Studio](https://obsproject.com/) (one-time setup to install the OBS Virtual Camera extension).
   - **Linux**: Install `v4l2loopback`:
     ```bash
     sudo apt install v4l2loopback-dkms
     sudo modprobe v4l2loopback devices=1 video_nr=10 card_label="StreamRelay" exclusive_caps=1
     ```
2. **SRT Transport (Optional)**:
   - Requires `ffmpeg` installed on your system PATH:
     ```bash
     # macOS
     brew install ffmpeg

     # Linux
     sudo apt install ffmpeg
     ```

---

## Quick Start (CLI)

Start the server with the interactive terminal dashboard:

```bash
streamrelay-server
```

Terminal output will display your local network addresses:
```
[streamrelay] HTTP   http://192.168.1.50:9091/
[streamrelay] HTTPS  https://192.168.1.50:9090/
[streamrelay] SRT    srt://192.168.1.50:9092
```

1. Connect your phone or laptop to the same Wi-Fi / local network.
2. Open the **HTTPS** URL in your browser (e.g., `https://192.168.1.50:9090/`).
3. Accept the self-signed TLS certificate (required by browsers for camera permissions):
   - **iOS Safari**: Tap *Show Details* → *visit this website*.
   - **Android Chrome**: Tap *Advanced* → *Proceed to ... (unsafe)*.
4. Tap **Start Streaming**. The video feed immediately streams to your server!

### CLI Keyboard Shortcuts

While the interactive CLI is running, use these single-key shortcuts:

| Key | Action |
|:---:|---|
| **`p`** | Toggle OpenCV local preview window on/off |
| **`v`** | Toggle virtual camera output on/off dynamically |
| **`s`** | Capture a high-resolution snapshot to `./snapshots/frame_NNNN.jpg` |
| **`r`** | Reset frame counters, stats, and clear terminal screen |
| **`q`** | Graceful shutdown |

*(These shortcuts also work while focused inside the OpenCV preview window!)*

### CLI Options

```bash
# Start with virtual camera enabled immediately
streamrelay-server --virtual-camera

# Start with OpenCV preview window opened immediately
streamrelay-server --preview

# Specify custom ports
streamrelay-server --https-port 8443 --http-port 8080 --srt-port 9092

# Specify a specific Linux virtual camera device
streamrelay-server --virtual-camera /dev/video10

# Disable rich terminal dashboard (plain log output)
streamrelay-server --no-stats
```

---

## Python Integration

StreamRelay is designed to integrate seamlessly into AI, computer vision, robotics, and GUI applications (OpenCV, PyTorch, YOLO, PyQt/PySide, Tkinter, etc.).

### 1. Consumer Application (`FrameReader`)

Any independent Python script or pipeline can consume frames directly from shared memory with zero network overhead:

```python
import time
import cv2
from streamrelay import FrameReader

# Connects to the shared memory block created by StreamServer
reader = FrameReader(attach_timeout=10.0)

print("Connected to StreamRelay frame buffer!")
while True:
    # Non-blocking read: returns latest HxWx3 uint8 BGR numpy array
    frame = reader.read_latest()
    if frame is None:
        time.sleep(0.005)
        continue

    # Process frame (e.g. inference, display, recording)
    # cv2.imshow("Stream", frame)
    # if cv2.waitKey(1) == 27:
    #     break

    # Optional: check if pipeline is falling behind
    if reader.skip_count > 2:
        print(f"Consumer slow: {reader.skip_count} frames overwritten")

reader.close()
```

#### Detecting *Only* New Frames

If your processing loop needs to avoid reprocessing identical frames:

```python
result = reader.read_new_with_info()
if result is not None:
    frame, info = result
    print(f"Frame #{info.counter}: {info.width}x{info.height}")
```

---

### 2. Embedding `StreamServer` in Your Python App

You can also run the server programmatically inside your own application.

#### Using Background Threads (e.g. PyQt / GUI apps)

```python
import threading
import time
from streamrelay import StreamServer

def frame_callback(frame_bgr, latency_ms, source, codec, byte_size):
    print(f"Received frame: {frame_bgr.shape}, latency: {latency_ms:.1f}ms, codec: {codec}")

server = StreamServer(
    http_port=9091,
    https_port=9090,
    srt_port=9092,
    virtual_camera=False,         # Can be enabled dynamically later
    on_frame=frame_callback,      # Optional direct callback
)

# Run server in a background daemon thread
server_thread = threading.Thread(target=server.run, daemon=True)
server_thread.start()

# Dynamically toggle virtual camera at any point in your app
enabled, msg = server.toggle_virtual_camera()
print(msg)  # "Virtual camera enabled"

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    server.stop()
```

---

## Architecture & Shared Memory Protocol

```
Mobile Browser / App
       │
       │  WebSocket (H.264 / JPEG)   wss://<ip>:9090/ws
       │  SRT       (H.264 MPEG-TS)  srt://<ip>:9092
       ▼
StreamServer (Producer)
       │
       ├─► VirtualCameraOutput   ──► OS Virtual Camera (OBS / v4l2loopback)
       ├─► on_frame() Callback   ──► In-process Python listeners
       └─► SharedMemory (POSIX)  ──► FrameReader (Consumer processes)
```

### Shared Memory Layout

StreamRelay allocates a fixed, lock-free memory-mapped segment (up to 4K / 3840×2160 BGR) using Python's standard `multiprocessing.shared_memory`:

| Byte Offset | Size | Type | Description |
|:---:|:---:|:---:|---|
| **0 – 3** | 4 B | `uint32` (LE) | Frame counter (incremented atomically on each write) |
| **4 – 7** | 4 B | `uint32` (LE) | Width in pixels |
| **8 – 11** | 4 B | `uint32` (LE) | Height in pixels |
| **12 – N** | `W×H×3` | `uint8` | Raw BGR image buffer (row-major contiguous) |

Because the header contains the exact dimensions of the active frame, consumers can adapt automatically when mobile orientation flips (portrait $\leftrightarrow$ landscape) or when stream resolution changes.

---

## Transports

| Transport | Default Port | Protocol | Best For |
|---|---|---|---|
| **WebSocket** | `9090` (HTTPS) / `9091` (HTTP) | TCP | Local Wi-Fi, low latency, mobile web browser |
| **SRT** | `9092` | UDP | Remote connections, unstable links, packet recovery |

---

## License

MIT License. See [LICENSE](LICENSE) for details.

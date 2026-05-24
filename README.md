# streamrelay

A lightweight Python package that streams camera frames from any phone or browser into your Python process over a local network — with near-zero latency and no external infrastructure.

```
┌──────────────────┐   WebSocket    ┌─────────────────┐  shared memory  ┌──────────────────┐
│  phone / browser │ ─────────────▶ │  streamrelay    │ ──────────────▶ │  your Python     │
│  (WebStreamer UI)│  JPEG / H.264  │  server         │                 │  process         │
└──────────────────┘                └─────────────────┘                 └──────────────────┘
```

## How it works

1. **Start the server** in a subprocess from your Python app. It binds an HTTP and HTTPS port on your machine.
2. **Open the URL** on any phone or browser on the same network. The bundled **WebStreamer** web UI loads automatically.
3. **Hit Start** in the UI. The browser captures the camera and streams encoded frames over a WebSocket to the server.
4. **Read frames** in your Python process using `FrameReader`. Each call returns a fresh BGR NumPy array ready for any OpenCV or ML pipeline.

No signaling servers. No STUN/TURN. No accounts. Works entirely on your local network.

---

## Features

- **Simple WebSocket transport** — no WebRTC negotiation, no SDP, works through any HTTP proxy
- **JPEG and H.264** — browser auto-selects the best codec; H.264 uses hardware acceleration where available
- **Shared-memory delivery** — the server writes decoded BGR frames into a named shared-memory block
- **Bundled WebStreamer UI** — phone opens a URL, grants camera permission, and streams immediately
- **Automatic self-signed TLS** — generated on first run so mobile browsers can access `getUserMedia`
- **Live reload** during development — edit the client files and the browser reloads automatically

---

## Install

```bash
# Core package
pip install streamrelay

# With H.264 decoding support (recommended)
pip install "streamrelay[h264]"

# Also auto-release ports on restart
pip install "streamrelay[h264,psutil]"
```

---

## Quick start

### Step 1 — Start the server

```python
import multiprocessing as mp
from streamrelay import StreamServer

def _serve():
    StreamServer(
        shm_name="myapp_frames",
        http_port=9091,
        https_port=9090,
    ).run()

if __name__ == "__main__":
    proc = mp.Process(target=_serve, daemon=True)
    proc.start()
```

### Step 2 — Open the WebStreamer UI

On any device on the same network, open:

```
https://<your-machine-ip>:9090/
```

Grant camera permission when prompted, select your preferred resolution and codec, then tap **Start Streaming**.

### Step 3 — Read frames in your process

```python
import time
from streamrelay import FrameReader

reader = FrameReader(shm_name="myapp_frames", attach_timeout=10.0)

while True:
    frame = reader.read_new()   # returns HxWx3 BGR ndarray, or None
    if frame is None:
        time.sleep(0.005)
        continue
    process(frame)
```

---

## StreamServer options

```python
StreamServer(
    shm_name="myapp_frames",   # shared-memory block name
    http_port=9091,            # plain HTTP port
    https_port=9090,           # HTTPS port (required for camera access on mobile)
    host="0.0.0.0",            # bind address
    cert_file="",              # path to existing TLS cert (auto-generated if empty)
    key_file="",               # path to existing TLS key
    on_frame=None,             # optional callback fn(frame_bgr)
)
```

---

## FrameReader options

```python
FrameReader(
    shm_name="myapp_frames",   # must match the server's shm_name
    attach_timeout=10.0,       # seconds to wait for the server
)
```

| Method | Returns | Description |
|---|---|---|
| `read_new()` | `ndarray \| None` | New frame since last read, or `None` |
| `read_latest()` | `ndarray \| None` | Most recent frame regardless of whether it is new |
| `close()` | — | Detach from shared memory |

---

## Shared-memory protocol

If you want to consume frames from a language other than Python, the shared-memory layout is:

| Bytes | Field | Type |
|---|---|---|
| 0–3 | counter | `uint32` little-endian |
| 4–7 | width | `uint32` little-endian |
| 8–11 | height | `uint32` little-endian |
| 12–N | pixels | BGR `uint8`, row-major |

---

## Development

```bash
git clone <this-repo>
cd packages/streamrelay
pip install -e ".[h264,psutil]"
streamrelay-server --shm-name dev_frames
```

---

## License

MIT.

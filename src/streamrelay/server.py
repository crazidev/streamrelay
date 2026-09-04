"""HTTP/HTTPS + WebSocket server that ingests camera frames from a browser
or a native Flutter app (via WebSocket or SRT).

Spawn this in a subprocess so it never blocks your app's event loop:

    import multiprocessing
    from streamrelay import StreamServer

    def _run():
        StreamServer(shm_name="my_frames").run()

    if __name__ == "__main__":
        multiprocessing.Process(target=_run, daemon=True).start()

The server:
* serves the bundled web UI at ``GET /``
* accepts JPEG or H.264 binary frames over ``GET /ws`` (WebSocket)
* accepts H.264 over SRT (port 9092 by default) for remote/Starlink use
* writes decoded BGR pixels into a named shared-memory block
* optionally outputs frames to a Linux v4l2loopback virtual camera device
* hot-reloads the web UI for development via SSE on ``/livereload``
"""

from __future__ import annotations

import asyncio
import json
import logging
import platform
import socket
import ssl
import threading
import time
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from typing import Callable, Optional

import cv2
import numpy as np
from aiohttp import web

from . import protocol

logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


# ── Default client assets bundled with the package ───────────────────────────
_PKG_DIR           = Path(__file__).parent
DEFAULT_CLIENT_DIR = _PKG_DIR / "client"


# ── Local IP helper ──────────────────────────────────────────────────────────
def _get_local_ip() -> str:
    """Best-effort: return the machine's LAN IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ── Virtual camera output (macOS, Linux, Windows) ─────────────────────────────
class VirtualCameraOutput:
    """Write incoming BGR frames to a virtual camera device with zero pipeline latency.

    Uses an asynchronous single-slot worker thread to prevent virtual camera driver
    I/O or conversions from ever blocking the video ingestion pipeline or network loop.

    Uses ``pyvirtualcam`` for cross-platform virtual camera support:
      - **macOS**: OBS Virtual Camera / CoreMediaIO Camera Extension.
      - **Linux**: ``v4l2loopback`` kernel module.
      - **Windows**: OBS Virtual Camera or UnityCapture.
    """

    def __init__(
        self,
        device: Optional[str] = None,
        backend: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        fps: float = 30.0,
    ) -> None:
        try:
            import pyvirtualcam
            self._pyvirtualcam = pyvirtualcam
        except ImportError as exc:
            raise ImportError(
                "pyvirtualcam not installed. Run: pip install pyvirtualcam"
            ) from exc

        # Handle strings like 'auto', '', True, backend names
        if isinstance(device, str):
            device_str = device.strip()
            if device_str.lower() in ("", "auto", "true", "1", "none"):
                device = None
            elif device_str.lower() in ("obs", "v4l2loopback", "unitycapture"):
                backend = device_str.lower()
                device = None

        self.device_req = device
        self.backend_req = backend
        self._fixed_size = (width is not None and height is not None)
        self._w = width or 0
        self._h = height or 0
        self._fps = fps
        self._cam: Optional[Any] = None
        self._lock = threading.Lock()

        # Decoupled async worker queue (single-slot: drops older frames if driver is busy)
        self._latest_slot: Optional[tuple[np.ndarray, float]] = None
        self._cond = threading.Condition()
        self._running = True
        self.latest_latency_ms: float = 0.0
        self.latest_send_time_ms: float = 0.0

        # If user explicitly requested fixed dimensions, initialize immediately
        if self._fixed_size and self._w > 0 and self._h > 0:
            try:
                self._init_cam(self._w, self._h)
            except Exception:
                self._cam = None

        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        self._init_failed = False

    @staticmethod
    def _fit_preserving_aspect(frame: np.ndarray, target_w: int, target_h: int) -> np.ndarray:
        """Fit frame into (target_w, target_h) preserving original aspect ratio (letterboxing/pillarboxing)."""
        fh, fw = frame.shape[:2]
        if fw == target_w and fh == target_h:
            return np.ascontiguousarray(frame)
        scale = min(target_w / fw, target_h / fh)
        nw = min(target_w, max(1, int(round(fw * scale))))
        nh = min(target_h, max(1, int(round(fh * scale))))
        resized = cv2.resize(frame, (nw, nh), interpolation=cv2.INTER_LINEAR)
        canvas = np.zeros((target_h, target_w, 3), dtype=np.uint8)
        x_off = (target_w - nw) // 2
        y_off = (target_h - nh) // 2
        canvas[y_off:y_off + nh, x_off:x_off + nw] = resized
        return canvas

    def _init_cam(self, width: int, height: int) -> None:
        # Align width to multiple of 16 and height to multiple of 2
        # This prevents macOS CVPixelBuffer / CoreMediaIO "Pixel buffer size mismatch" errors.
        width = ((int(width) + 15) // 16) * 16
        height = ((int(height) + 1) // 2) * 2

        if self._cam is not None:
            try:
                self._cam.close()
            except Exception:
                pass
            self._cam = None

        kwargs: dict = {
            "width": width,
            "height": height,
            "fps": self._fps,
            "fmt": self._pyvirtualcam.PixelFormat.BGR,
        }
        if self.device_req:
            kwargs["device"] = self.device_req
        if self.backend_req:
            kwargs["backend"] = self.backend_req

        try:
            self._cam = self._pyvirtualcam.Camera(**kwargs)
            self._w = width
            self._h = height
            print(
                f"[streamrelay] Virtual camera active: {self._cam.device} "
                f"({self._cam.backend}, {self._w}×{self._h} @ {self._fps}fps)"
            )
        except Exception as exc:
            plat = platform.system()
            if plat == "Linux":
                hint = (
                    "Make sure v4l2loopback is loaded:\n"
                    "  sudo modprobe v4l2loopback devices=1 video_nr=10 card_label=\"StreamRelay\" exclusive_caps=1"
                )
            elif plat == "Darwin":
                hint = (
                    "Make sure OBS Studio is installed or OBS Virtual Camera is enabled in macOS System Settings > Privacy & Security > Extensions."
                )
            else:
                hint = "Ensure virtual camera driver (OBS or UnityCapture) is installed."
            raise RuntimeError(f"Failed to initialize virtual camera: {exc}\n[streamrelay] {hint}") from exc

    def _worker_loop(self) -> None:
        while self._running:
            item = None
            with self._cond:
                while self._running and self._latest_slot is None:
                    self._cond.wait(timeout=0.1)
                if not self._running:
                    break
                item = self._latest_slot
                self._latest_slot = None

            if item is None or not self._running:
                continue

            frame_bgr, capture_ts = item
            try:
                h, w = frame_bgr.shape[:2]
                with self._lock:
                    if self._cam is None and not self._init_failed:
                        try:
                            self._init_cam(w, h)
                        except Exception as exc:
                            self._init_failed = True
                            print(f"[streamrelay] {exc}")

                    if self._cam is not None:
                        cam_w = self._cam.width
                        cam_h = self._cam.height
                        if w != cam_w or h != cam_h:
                            frame_to_send = self._fit_preserving_aspect(frame_bgr, cam_w, cam_h)
                        else:
                            frame_to_send = np.ascontiguousarray(frame_bgr)

                        t0 = time.perf_counter()
                        self._cam.send(frame_to_send)
                        self.latest_send_time_ms = (time.perf_counter() - t0) * 1000.0

                now_ms = time.time() * 1000.0
                if capture_ts > 0:
                    self.latest_latency_ms = max(0.0, now_ms - capture_ts)
            except Exception:
                pass

    def write(self, frame_bgr: np.ndarray, capture_ts: Optional[float] = None) -> None:
        """Non-blocking: pushes the newest frame into the single-slot buffer."""
        if frame_bgr is None or frame_bgr.size == 0 or not self._running:
            return
        with self._cond:
            self._latest_slot = (frame_bgr, capture_ts or (time.time() * 1000.0))
            self._cond.notify()

    def reset(self) -> None:
        """Reset virtual camera so next frame can re-initialize resolution."""
        if self._fixed_size:
            return
        with self._lock:
            self._init_failed = False
            if self._cam is not None:
                try:
                    self._cam.close()
                except Exception:
                    pass
                self._cam = None

    def close(self) -> None:
        self._running = False
        with self._cond:
            self._cond.notify_all()
        if self._worker_thread.is_alive() and threading.current_thread() != self._worker_thread:
            self._worker_thread.join(timeout=0.5)
        with self._lock:
            if self._cam is not None:
                try:
                    self._cam.close()
                except Exception:
                    pass
                self._cam = None

    def __enter__(self) -> "VirtualCameraOutput":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()




# ── SRT listener (ffmpeg engine) ──────────────────────────────────────────────
AudioCallback = Callable[[bytes, dict], None]


class SRTListener:
    """Accept H.264 video over SRT using ffmpeg as the transport engine.

    SRT (Secure Reliable Transport) is UDP-based with selective
    retransmission and a configurable latency window. Packets that cannot
    be retransmitted within ``latency_ms`` are dropped — the consumer
    always receives current frames, never stale ones.

    Designed for remote / Starlink links where TCP head-of-line blocking
    would cause visible stalls. For LAN use the existing WebSocket path.

    Uses ``ffmpeg`` (which bundles libsrt) as the receive engine.
    No extra Python SRT packages are required.

    Requires:
        ffmpeg built with ``--enable-libsrt`` (most distribution binaries
        include it; verify with ``ffmpeg -protocols | grep srt``).

    Parameters
    ----------
    port:
        UDP port to listen on (default 9092).
    host:
        Bind address (default all interfaces).
    latency_ms:
        SRT latency buffer in milliseconds. 200 ms covers Starlink jitter
        and satellite handoffs (~90 s cycle).
    on_frame:
        Callback ``fn(frame_bgr)`` invoked for every decoded video frame.
    on_audio:
        Optional callback ``fn(chunk_bytes, config_dict)`` for audio data.
    """

    def __init__(
        self,
        port: int = 9092,
        host: str = "0.0.0.0",
        latency_ms: int = 200,
        on_frame: Optional[FrameCallback] = None,
        on_audio: Optional[AudioCallback] = None,
    ) -> None:
        self.port = port
        self.host = host
        self.latency_ms = latency_ms
        self.on_frame = on_frame
        self.on_audio = on_audio
        self._running = False
        self._proc: Optional[subprocess.Popen] = None
        self._probe_proc: Optional[subprocess.Popen] = None

    def run(self) -> None:
        """Block the current thread; single-client SRT listener session."""
        import shutil
        if not shutil.which("ffmpeg"):
            print(
                "[streamrelay/SRT] ffmpeg not found in PATH. "
                "Install ffmpeg to enable SRT transport."
            )
            return

        self._running = True
        # SRT latency is specified in microseconds in the URL parameter
        srt_url = (
            f"srt://{self.host}:{self.port}"
            f"?mode=listener&latency={self.latency_ms * 1000}"
        )
        print(
            f"[streamrelay/SRT] Listening on {self.host}:{self.port} "
            f"(latency={self.latency_ms}ms, ffmpeg engine)"
        )
        while self._running:
            had_connection = self._run_once(srt_url)
            if not self._running:
                break
            if not had_connection:
                time.sleep(1.0)

    def stop(self) -> None:
        self._running = False
        if self._proc is not None:
            try:
                self._proc.terminate()
                self._proc.kill()
            except Exception:
                pass
            self._proc = None

    def _run_once(self, srt_url: str) -> bool:
        """Run one ffmpeg receive session; return True if a client connected."""
        import subprocess

        width, height = 1280, 720
        frame_bytes = width * height * 3  # BGR24

        # Start ffmpeg listening on the SRT port with low-latency flags
        try:
            proc = subprocess.Popen(
                [
                    "ffmpeg", "-loglevel", "error",
                    "-fflags", "nobuffer",
                    "-flags", "low_delay",
                    "-probesize", "32",
                    "-analyzeduration", "0",
                    "-i", srt_url,
                    "-f", "rawvideo", "-pix_fmt", "bgr24",
                    "-vf", f"scale={width}:{height}",
                    "pipe:1",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._proc = proc
        except Exception as exc:
            print(f"[streamrelay/SRT] Failed to start ffmpeg: {exc}")
            time.sleep(1.0)
            return False

        frame_count = 0
        start_time  = time.time()
        last_log    = start_time
        buf         = bytearray()
        connected   = False

        try:
            while self._running and proc.poll() is None:
                needed = frame_bytes - len(buf)
                chunk  = proc.stdout.read(needed)  # type: ignore[union-attr]
                if not chunk:
                    break
                buf.extend(chunk)
                if len(buf) < frame_bytes:
                    continue
                ingest_ts = time.time() * 1000.0
                frame_bgr = (
                    np.frombuffer(bytes(buf[:frame_bytes]), dtype=np.uint8)
                    .reshape((height, width, 3))
                    .copy()
                )
                buf = buf[frame_bytes:]
                if not connected:
                    connected = True
                    print(f"[streamrelay/SRT] Client connected ({width}×{height})")
                if self.on_frame is not None:
                    try:
                        self.on_frame(frame_bgr, ingest_ts, "srt", "h264")
                    except TypeError:
                        try:
                            self.on_frame(frame_bgr, ingest_ts, "srt")
                        except TypeError:
                            try:
                                self.on_frame(frame_bgr, ingest_ts)
                            except TypeError:
                                self.on_frame(frame_bgr)
                frame_count += 1

                now = time.time()
                if now - last_log >= 5.0:
                    fps = frame_count / (now - start_time)
                    print(
                        f"[streamrelay/SRT] {frame_count} frames, "
                        f"{fps:.1f} FPS ({width}×{height})"
                    )
                    last_log = now
        except Exception:
            pass
        finally:
            if self._proc is not None:
                try:
                    self._proc.terminate()
                    self._proc.kill()
                except Exception:
                    pass
                self._proc = None

            if connected:
                elapsed = time.time() - start_time
                avg_fps = frame_count / elapsed if elapsed else 0
                print(
                    f"[streamrelay/SRT] Client disconnected — {frame_count} frames "
                    f"in {elapsed:.1f}s ({avg_fps:.1f} FPS)"
                )
            elif self._running:
                time.sleep(1.0)
        return connected



# ── Shared-memory helpers ────────────────────────────────────────────────────
def _create_shm(name: str) -> SharedMemory:
    """Create shared memory block. If an existing one exists with mismatched size or from an earlier run, recreate it."""
    try:
        shm = SharedMemory(name=name, create=True, size=protocol.SHM_TOTAL_BYTES)
        shm.buf[:protocol.SHM_TOTAL_BYTES] = b"\x00" * protocol.SHM_TOTAL_BYTES
        return shm
    except FileExistsError:
        try:
            existing = SharedMemory(name=name, create=False)
            if existing.size < protocol.SHM_TOTAL_BYTES:
                existing.close()
                existing.unlink()
                shm = SharedMemory(name=name, create=True, size=protocol.SHM_TOTAL_BYTES)
                shm.buf[:protocol.SHM_TOTAL_BYTES] = b"\x00" * protocol.SHM_TOTAL_BYTES
                return shm
            return existing
        except Exception:
            # Fallback: create with different or default
            shm = SharedMemory(name=name, create=False)
            return shm


def _write_frame(shm: SharedMemory, frame_bgr: np.ndarray) -> None:
    """Write frame to shared memory with a header containing (counter, width, height)."""
    try:
        max_bytes = max(0, len(shm.buf) - protocol.SHM_HEADER_BYTES)
        h, w = frame_bgr.shape[:2]
        frame_bytes = w * h * 3

        if frame_bytes > max_bytes:
            # Frame exceeds SHM buffer: downscale to fit inside max_bytes
            scale = (max_bytes / frame_bytes) ** 0.5
            nw = max(1, int(w * scale))
            nh = max(1, int(h * scale))
            frame_bgr = cv2.resize(frame_bgr, (nw, nh), interpolation=cv2.INTER_LINEAR)
            h, w = frame_bgr.shape[:2]

        counter, _, _ = protocol.unpack_header(shm.buf)
        counter = (counter + 1) & 0xFFFFFFFF
        protocol.pack_header(shm.buf, counter, w, h)
        pixel_bytes = np.ascontiguousarray(frame_bgr).tobytes()
        write_len = min(len(pixel_bytes), max_bytes)
        shm.buf[protocol.SHM_HEADER_BYTES:
                protocol.SHM_HEADER_BYTES + write_len] = pixel_bytes[:write_len]
    except Exception:
        pass


def _kill_process_on_port(port: int) -> None:
    """Best-effort: kill any process holding the given port (TCP or UDP)."""
    try:
        import psutil
        import signal
    except ImportError:
        return
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            connections = (
                proc.net_connections() if hasattr(proc, "net_connections")
                else proc.connections()
            )
            for conn in connections:
                if conn.laddr and conn.laddr.port == port:
                    print(f"[streamrelay] Releasing port {port} from PID {proc.pid} ({proc.name()})")
                    try:
                        proc.send_signal(signal.SIGTERM)
                        proc.wait(timeout=1.5)
                    except Exception:
                        proc.kill()
        except Exception:
            pass


# ── TLS certificate helper ───────────────────────────────────────────────────
def generate_self_signed_cert(
    cert_path: Path,
    key_path: Path,
    common_name: str = "streamrelay.local",
    organization: str = "streamrelay",
) -> None:
    """Generate a self-signed certificate so phones can use getUserMedia
    (which requires HTTPS for non-localhost origins)."""
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    import datetime
    import ipaddress
    import socket

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    print("[streamrelay] Generating self-signed SSL certificate…")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    hostname = socket.gethostname()
    local_ips = ["127.0.0.1", "0.0.0.0"]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ips.append(s.getsockname()[0])
        s.close()
    except Exception:
        pass

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization),
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])
    san_entries = [x509.DNSName("localhost"), x509.DNSName(hostname)]
    for ip in local_ips:
        try:
            san_entries.append(x509.IPAddress(ipaddress.IPv4Address(ip)))
        except ValueError:
            pass

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow() - datetime.timedelta(days=1))
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=3650))
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .sign(key, hashes.SHA256())
    )
    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    print(f"[streamrelay] SSL certificate written to {cert_path}")


# ── The server itself ────────────────────────────────────────────────────────
FrameCallback = Callable[[np.ndarray], None]


# ── SSL fingerprint helper ───────────────────────────────────────────────────
def _print_cert_fingerprint(cert_path: Path) -> None:
    """Print the SHA-256 fingerprint of a PEM certificate."""
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes
        import binascii
        pem = cert_path.read_bytes()
        cert = x509.load_pem_x509_certificate(pem)
        fp = cert.fingerprint(hashes.SHA256())
        chunks = [binascii.hexlify(fp[i:i+1]).decode().upper()
                  for i in range(len(fp))]
        fp_str = ":".join(chunks)
        print(f"[streamrelay] TLS fingerprint (SHA-256):")
        print(f"              {fp_str}")
    except Exception:
        pass  # Non-fatal


class StreamServer:
    """A bundle of HTTP + HTTPS + WebSocket endpoints that turns a phone
    or browser camera into BGR frames in a shared-memory block.

    Parameters
    ----------
    shm_name:
        Name of the shared-memory block to create. Must be unique per host.
    http_port, https_port:
        Ports to bind. Set ``https_port=0`` to disable TLS.
    host:
        Bind address. Defaults to all interfaces.
    cert_file, key_file:
        Paths to an existing TLS cert/key. If empty, a self-signed pair
        is generated next to the bundled client folder.
    client_dir:
        Override the static-asset folder. Defaults to the bundled UI.
    on_frame:
        Optional callback ``fn(frame_bgr) -> None`` invoked for every
        decoded frame **in addition to** the shared-memory write. Useful
        for in-process consumers that don't need the shm dance.
    """

    def __init__(
        self,
        shm_name: str = protocol.DEFAULT_SHM_NAME,
        http_port: int = 9091,
        https_port: int = 9090,
        srt_port: int = 9092,
        host: str = "0.0.0.0",
        cert_file: str = "",
        key_file: str = "",
        cert_dir: str = "",
        client_dir: Optional[Path] = None,
        on_frame: Optional[FrameCallback] = None,
        on_audio: Optional[AudioCallback] = None,
        virtual_camera: str = "",
    ):
        self.shm_name = shm_name
        self.http_port = http_port
        self.https_port = https_port
        self.srt_port = srt_port
        self.host = host
        self.cert_file = cert_file
        self.key_file = key_file
        self.cert_dir = cert_dir
        self.client_dir = Path(client_dir) if client_dir else DEFAULT_CLIENT_DIR
        self.on_audio = on_audio
        self.virtual_camera = virtual_camera
        self._virtual_cam: Optional[VirtualCameraOutput] = None
        self._clock_offset_ms: float = 0.0
        self.latest_latency_ms: float = 0.0

        self.on_frame = on_frame
        if virtual_camera:
            try:
                dev = virtual_camera if isinstance(virtual_camera, str) else None
                self._virtual_cam = VirtualCameraOutput(device=dev)
            except Exception as exc:
                print(f"[streamrelay] Virtual camera disabled: {exc}")

        self._shm: Optional[SharedMemory] = None
        self._file_mtimes: dict = {}
        self._reload_clients: list = []
        self._srt_listener: Optional[SRTListener] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._runner: Optional[web.AppRunner] = None

    def enable_virtual_camera(self, device: Optional[str] = None) -> tuple[bool, str]:
        """Enable virtual camera output dynamically."""
        if self._virtual_cam is not None:
            return True, "Virtual camera is already active"
        try:
            dev = device or (self.virtual_camera if isinstance(self.virtual_camera, str) else None)
            self._virtual_cam = VirtualCameraOutput(device=dev)
            self.virtual_camera = dev or "auto"
            return True, "Virtual camera enabled"
        except Exception as exc:
            self._virtual_cam = None
            self.virtual_camera = None
            return False, f"Virtual camera error: {exc}"

    def disable_virtual_camera(self) -> tuple[bool, str]:
        """Disable virtual camera output dynamically."""
        if self._virtual_cam is not None:
            try:
                self._virtual_cam.close()
            except Exception:
                pass
            self._virtual_cam = None
        self.virtual_camera = None
        return False, "Virtual camera disabled"

    def toggle_virtual_camera(self, device: Optional[str] = None) -> tuple[bool, str]:
        """Toggle virtual camera output on or off."""
        if self._virtual_cam is not None:
            return self.disable_virtual_camera()
        return self.enable_virtual_camera(device)

    def stop(self) -> None:
        """Stop all listeners, web servers, and release shared memory."""
        if self._virtual_cam is not None:
            try:
                self._virtual_cam.close()
            except Exception:
                pass
            self._virtual_cam = None
        if self._srt_listener is not None:
            self._srt_listener.stop()
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._shm is not None:
            try:
                self._shm.close()
                self._shm.unlink()
            except Exception:
                pass
            self._shm = None

    # ── Static handlers ──────────────────────────────────────────────────────
    async def _index(self, request: web.Request) -> web.Response:
        path = self.client_dir / "index.html"
        return web.Response(content_type="text/html",
                            text=path.read_text(encoding="utf-8"))

    async def _javascript(self, request: web.Request) -> web.Response:
        path = self.client_dir / "app.js"
        return web.Response(content_type="application/javascript",
                            text=path.read_text(encoding="utf-8"))

    async def _css(self, request: web.Request) -> web.Response:
        path = self.client_dir / "style.css"
        return web.Response(content_type="text/css",
                            text=path.read_text(encoding="utf-8"))

    # ── Frame ingestion (WebSocket) ──────────────────────────────────────────
    async def _ws_stream(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(max_msg_size=5 * 1024 * 1024)
        await ws.prepare(request)
        print("[streamrelay] Client connected")

        frame_count = 0
        codec = "jpeg"
        h264_decoder = None
        start_time = time.time()
        last_log = start_time
        h264_errors = 0
        audio_config: dict = {}
        audio_mode = False

        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    if msg.data == "ping":
                        await ws.send_str("pong")
                        continue
                    try:
                        config = json.loads(msg.data)
                    except (json.JSONDecodeError, ValueError):
                        continue

                    msg_type = config.get("type")

                    if msg_type in ("ping", "sync"):
                        client_t = float(config.get("t", 0))
                        server_t = time.time() * 1000.0
                        self._clock_offset_ms = server_t - client_t
                        await ws.send_str(json.dumps({
                            "type": "pong" if msg_type == "ping" else "sync_ack",
                            "client_t": client_t,
                            "server_t": server_t,
                            "offset": self._clock_offset_ms,
                        }))
                        continue

                    if msg_type == "audio":
                        # Audio stream config — subsequent binary frames may
                        # be audio chunks (identified by 0x02 leading byte).
                        audio_config = config
                        audio_mode = True
                        print(f"[streamrelay] Audio stream: "
                              f"{config.get('codec','?')} "
                              f"{config.get('sample_rate','?')}Hz "
                              f"{config.get('channels','?')}ch")
                        continue

                    if msg_type == "codec":
                        codec = config.get("codec", "jpeg")
                        w = config.get("width", 1280)
                        h = config.get("height", 720)
                        print(f"[streamrelay] Codec: {codec}, "
                              f"resolution: {w}x{h}")
                        if codec == "h264":
                            try:
                                import av  # type: ignore
                                h264_decoder = av.CodecContext.create("h264", "r")
                                h264_decoder.extradata = None
                                print("[streamrelay] H.264 decoder ready (PyAV)")
                            except ImportError:
                                print(
                                    "[streamrelay] PyAV not installed — H.264 "
                                    "unavailable.  Run: pip install av\n"
                                    "[streamrelay] Falling back to JPEG."
                                )
                                codec = "jpeg"
                                await ws.send_str(json.dumps(
                                    {"type": "fallback", "codec": "jpeg"}))

                elif msg.type == web.WSMsgType.BINARY:
                    data = msg.data

                    # Check for tagged frame (first byte = type tag):
                    # 0x01 = video untagged/standard
                    # 0x02 = audio
                    # 0x03 = video with 8-byte uint64 capture timestamp (ms)
                    capture_ts = None
                    if len(data) >= 9 and data[0] == 0x03:
                        import struct
                        tag = 0x01
                        capture_ts = float(struct.unpack(">Q", data[1:9])[0])
                        payload = data[9:]
                    elif len(data) > 1 and data[0] in (0x01, 0x02):
                        tag = data[0]
                        payload = data[1:]
                    else:
                        tag = 0x01  # untagged = video (backwards compat)
                        payload = data

                    if tag == 0x02:
                        # Audio chunk — route to on_audio callback
                        if self.on_audio is not None:
                            try:
                                self.on_audio(bytes(payload), audio_config)
                            except Exception as exc:
                                print(f"[streamrelay] on_audio raised: {exc}")
                        continue

                    # Video frame
                    wire_bytes = len(data)
                    if codec == "h264" and h264_decoder is not None:
                        try:
                            import av  # type: ignore
                            packet = av.Packet(payload)
                            for frame in h264_decoder.decode(packet):
                                img = frame.to_ndarray(format="bgr24")
                                self._dispatch_frame(
                                    img, capture_ts, source="ws", codec="h264", byte_size=wire_bytes
                                )
                                frame_count += 1
                                h264_errors = 0
                        except Exception:
                            h264_errors += 1
                            if h264_errors > 30:
                                print("[streamrelay] Too many H.264 errors; "
                                      "switching to JPEG")
                                codec = "jpeg"
                                h264_decoder = None
                                await ws.send_str(json.dumps(
                                    {"type": "fallback", "codec": "jpeg"}))
                    else:
                        frame_bgr = cv2.imdecode(
                            np.frombuffer(payload, dtype=np.uint8),
                            cv2.IMREAD_COLOR,
                        )
                        if frame_bgr is not None:
                            # Auto-detect JPEG SOI marker or fallback to current codec
                            is_jpeg = len(payload) >= 3 and payload[0] == 0xFF and payload[1] == 0xD8
                            c_name = "jpeg" if is_jpeg else codec
                            self._dispatch_frame(
                                frame_bgr, capture_ts, source="ws", codec=c_name, byte_size=wire_bytes
                            )
                            frame_count += 1

                    now = time.time()
                    if now - last_log >= 5.0:
                        elapsed = now - start_time
                        fps = frame_count / elapsed if elapsed else 0
                        print(f"[streamrelay] {frame_count} frames, "
                              f"{fps:.1f} FPS avg ({codec})")
                        last_log = now

                elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                    break
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"[streamrelay] WS error: {e}")
        finally:
            elapsed = time.time() - start_time
            avg_fps = frame_count / elapsed if elapsed else 0
            print(f"[streamrelay] Disconnected — {frame_count} frames in "
                  f"{elapsed:.1f}s ({avg_fps:.1f} FPS)")
            if self._virtual_cam is not None:
                self._virtual_cam.reset()
        return ws

    def _dispatch_frame(
        self,
        frame_bgr: np.ndarray,
        capture_ts: Optional[float] = None,
        source: str = "ws",
        codec: str = "—",
        byte_size: Optional[int] = None,
    ) -> None:
        now_ms = time.time() * 1000.0
        if capture_ts is not None:
            if source == "ws":
                creation_ts = capture_ts + self._clock_offset_ms
            else:
                creation_ts = capture_ts
            latency_ms = max(0.0, now_ms - creation_ts)
        else:
            creation_ts = now_ms
            latency_ms = 0.0
        self.latest_latency_ms = latency_ms
        if codec and codec != "—":
            self.latest_codec = codec

        if self._shm is not None:
            try:
                _write_frame(self._shm, frame_bgr)
            except Exception:
                pass
        if self._virtual_cam is not None:
            self._virtual_cam.write(frame_bgr, creation_ts)
        if self.on_frame is not None:
            try:
                self.on_frame(frame_bgr, latency_ms, source, codec, byte_size)
            except TypeError:
                try:
                    self.on_frame(frame_bgr, latency_ms, source, codec)
                except TypeError:
                    try:
                        self.on_frame(frame_bgr, latency_ms, source)
                    except TypeError:
                        try:
                            self.on_frame(frame_bgr, latency_ms)
                        except TypeError:
                            self.on_frame(frame_bgr)
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(f"[streamrelay] on_frame callback raised: {e}")

    # ── Live-reload (development convenience) ────────────────────────────────
    def _scan_client_files(self) -> dict:
        out: dict = {}
        if self.client_dir.is_dir():
            for f in self.client_dir.iterdir():
                if f.is_file():
                    out[str(f)] = f.stat().st_mtime
        return out

    async def _file_watcher_task(self) -> None:
        self._file_mtimes = self._scan_client_files()
        while True:
            await asyncio.sleep(1)
            current = self._scan_client_files()
            if current != self._file_mtimes:
                self._file_mtimes = current
                for q in self._reload_clients:
                    await q.put("reload")

    async def _livereload_sse(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200, reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
            },
        )
        await response.prepare(request)
        queue: asyncio.Queue = asyncio.Queue()
        self._reload_clients.append(queue)
        try:
            await response.write(b": heartbeat\n\n")
            while True:
                msg = await queue.get()
                await response.write(f"data: {msg}\n\n".encode())
        except (asyncio.CancelledError, ConnectionResetError, ConnectionError):
            pass
        finally:
            self._reload_clients.remove(queue)
        return response

    # ── App lifecycle ────────────────────────────────────────────────────────
    async def _on_startup(self, app: web.Application) -> None:
        app["file_watcher"] = asyncio.ensure_future(self._file_watcher_task())

    async def _on_shutdown(self, app: web.Application) -> None:
        watcher = app.get("file_watcher")
        if watcher:
            watcher.cancel()
            try:
                await watcher
            except asyncio.CancelledError:
                pass
        if self._shm is not None:
            self._shm.close()

    # ── Entry points ─────────────────────────────────────────────────────────
    def run(self) -> None:
        """Block the current process and serve until interrupted."""
        if self.http_port:
            _kill_process_on_port(self.http_port)
        if self.https_port:
            _kill_process_on_port(self.https_port)
        if self.srt_port:
            _kill_process_on_port(self.srt_port)

        self._shm = _create_shm(self.shm_name)

        # ── Start SRT listener thread (if enabled) ────────────────────────
        if self.srt_port:
            self._srt_listener = SRTListener(
                port=self.srt_port,
                host=self.host,
                on_frame=self._dispatch_frame,
                on_audio=self.on_audio,
            )
            srt_thread = threading.Thread(
                target=self._srt_listener.run, daemon=True
            )
            srt_thread.start()

        app = web.Application()
        app.on_startup.append(self._on_startup)
        app.on_shutdown.append(self._on_shutdown)

        app.router.add_get("/", self._index)
        app.router.add_get("/app.js", self._javascript)
        app.router.add_get("/style.css", self._css)
        app.router.add_get("/ws", self._ws_stream)
        app.router.add_get("/livereload", self._livereload_sse)

        ssl_ctx, cert_path = self._build_ssl_context()
        local_ip = _get_local_ip()

        async def start_servers():
            runner = web.AppRunner(
                app,
                max_line_size=65536,
                max_field_size=65536,
                max_headers=256,
            )
            self._runner = runner
            await runner.setup()
            if self.http_port:
                http_site = web.TCPSite(runner, self.host, self.http_port)
                await http_site.start()
                print(f"[streamrelay] HTTP   http://{local_ip}:{self.http_port}/")
            if ssl_ctx and self.https_port:
                https_site = web.TCPSite(
                    runner, self.host, self.https_port, ssl_context=ssl_ctx
                )
                await https_site.start()
                https_url = f"https://{local_ip}:{self.https_port}/"
                print(f"[streamrelay] HTTPS  {https_url}")
                if cert_path:
                    _print_cert_fingerprint(cert_path)
            if self.srt_port:
                print(f"[streamrelay] SRT    srt://{local_ip}:{self.srt_port}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(start_servers())
            loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            self.stop()
            try:
                if self._runner is not None and not loop.is_closed():
                    loop.run_until_complete(self._runner.cleanup())
            except Exception:
                pass
            try:
                if not loop.is_closed():
                    loop.close()
            except Exception:
                pass


    # ── Helpers ──────────────────────────────────────────────────────────────
    def _build_ssl_context(
        self,
    ) -> tuple[Optional[ssl.SSLContext], Optional[Path]]:
        """Build SSL context. Returns ``(ctx, cert_path)`` or ``(None, None)``."""
        if self.https_port == 0:
            return None, None
        cert_file = self.cert_file
        key_file = self.key_file
        if not cert_file or not key_file:
            # Resolve cert directory: --cert-dir > CWD/streamrelay-certs
            if self.cert_dir:
                base = Path(self.cert_dir)
            else:
                base = Path.cwd() / "streamrelay-certs"
            cert_file = str(base / "cert.pem")
            key_file = str(base / "key.pem")
        cert_path = Path(cert_file)
        key_path = Path(key_file)
        if not (cert_path.is_file() and key_path.is_file()):
            try:
                generate_self_signed_cert(cert_path, key_path)
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(
                    f"[streamrelay] Cert generation failed: {e}\n"
                    "[streamrelay] Hint: pip install cryptography"
                )
        if cert_path.is_file() and key_path.is_file():
            try:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ctx.load_cert_chain(str(cert_path), str(key_path))
                return ctx, cert_path
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(f"[streamrelay] SSL load error: {e}")
        return None, None


# ── Backwards-compatible function form ───────────────────────────────────────
def run_server(
    https_port: int = 9090,
    http_port: int = 9091,
    cert_file: str = "",
    key_file: str = "",
    host: str = "0.0.0.0",
    shm_name: str = protocol.DEFAULT_SHM_NAME,
) -> None:
    """Functional wrapper kept for legacy callers."""
    StreamServer(
        shm_name=shm_name,
        http_port=http_port,
        https_port=https_port,
        host=host,
        cert_file=cert_file,
        key_file=key_file,
    ).run()


if __name__ == "__main__":
    StreamServer().run()


def _cli() -> None:
    """Console-script entry point: ``streamrelay-server`` after pip install."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="streamrelay-server",
        description=(
            "Run a streamrelay server — WebSocket (JPEG/H.264) + "
            "SRT (H.264) video ingestion with shared-memory delivery."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--shm-name", default=protocol.DEFAULT_SHM_NAME,
        help="Name of the shared-memory block",
    )
    parser.add_argument("--http-port",  type=int, default=9091,
                        help="HTTP port (0 = disable)")
    parser.add_argument("--https-port", type=int, default=9090,
                        help="HTTPS port (0 = disable)")
    parser.add_argument("--srt-port",   type=int, default=9092,
                        help="SRT UDP port (0 = disable)")
    parser.add_argument("--host",        default="0.0.0.0",
                        help="Bind address")
    parser.add_argument("--cert-file",   default="",
                        help="Path to TLS certificate (auto-generated if empty)")
    parser.add_argument("--key-file",    default="",
                        help="Path to TLS private key")
    parser.add_argument("--cert-dir",    default="",
                        help="Directory for auto-generated certs "
                             "(default: ./streamrelay-certs/)")
    parser.add_argument(
        "--virtual-camera", nargs="?", const="auto", default="", metavar="DEVICE",
        help="Enable virtual camera output (auto, OBS on macOS, or /dev/video10 on Linux)",
    )
    args = parser.parse_args()

    StreamServer(
        shm_name=args.shm_name,
        http_port=args.http_port,
        https_port=args.https_port,
        srt_port=args.srt_port,
        host=args.host,
        cert_file=args.cert_file,
        key_file=args.key_file,
        cert_dir=args.cert_dir,
        virtual_camera=args.virtual_camera,
    ).run()

"""Interactive CLI for streamrelay-server.

Provides a rich live-stats dashboard with keyboard shortcuts:

    [p]  Toggle OpenCV preview window
    [v]  Toggle virtual camera streaming
    [s]  Snapshot — save current frame to ./snapshots/frame_NNNN.jpg
    [r]  Reset frame counter, stats, and clear console logs
    [q]  Graceful shutdown

Usage::

    streamrelay-server                    # full interactive UI
    streamrelay-server --no-stats         # plain log output (no rich)
    streamrelay-server --preview          # launch preview immediately
    streamrelay-server --srt-port 9092
    streamrelay-server --virtual-camera /dev/video10
"""

from __future__ import annotations

import argparse
import os
import select
import sys
import termios
import threading
import time
import tty
from collections import deque
from pathlib import Path
from typing import Optional

import numpy as np

from . import protocol
from .reader import FrameReader
from .server import StreamServer, VirtualCameraOutput, _get_local_ip


# ── Stats collector ──────────────────────────────────────────────────────────
class _TransportStats:
    """Thread-safe per-transport statistics."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._lock = threading.RLock()
        self.frame_count: int = 0
        self.byte_count: int = 0
        self.codec: str = "—"
        self.resolution: str = "—"
        self.connected: bool = False
        self._start: float = time.time()
        self._window_frames: list[float] = []   # timestamps of last N frames
        self._window_bytes:  list[int]   = []
        self.latency_ms: float = 0.0
        self.vcam_latency_ms: float = 0.0

    def record_frame(
        self,
        frame_bgr: np.ndarray,
        latency_ms: float = 0.0,
        vcam_latency_ms: float = 0.0,
        codec: str = "—",
        byte_size: Optional[int] = None,
    ) -> None:
        with self._lock:
            now = time.time()
            self.frame_count += 1
            if byte_size is not None and byte_size > 0:
                nb = byte_size
            else:
                # If network byte size is unknown (e.g. raw decoded pipe from SRT),
                # estimate realistic compressed H.264 packet size (~2.5 Mbps) rather than
                # uncompressed raw BGR memory size in RAM which distorts stats to 200+ Mbps
                nb = int(frame_bgr.nbytes * 0.015) if codec == "H.264" else int(frame_bgr.nbytes * 0.08)
            self.byte_count += nb
            self.connected = True
            self.latency_ms = latency_ms
            self.vcam_latency_ms = vcam_latency_ms
            if codec and codec != "—":
                c = codec.strip().lower()
                self.codec = "H.264" if c in ("h264", "h.264", "avc") else "JPEG"
            elif self.codec == "—":
                self.codec = "JPEG"
            h, w = frame_bgr.shape[:2]
            self.resolution = f"{w}×{h}"
            # Keep a 5-second sliding window for FPS / bitrate
            cutoff = now - 5.0
            self._window_frames.append(now)
            self._window_bytes.append(nb)
            while self._window_frames and self._window_frames[0] < cutoff:
                self._window_frames.pop(0)
                self._window_bytes.pop(0)

    def set_codec(self, codec: str) -> None:
        with self._lock:
            self.codec = codec

    def mark_disconnected(self) -> None:
        with self._lock:
            self.connected = False

    @property
    def fps(self) -> float:
        with self._lock:
            n = len(self._window_frames)
            if n < 2:
                return 0.0
            return (n - 1) / (self._window_frames[-1] - self._window_frames[0])

    @property
    def bitrate_mbps(self) -> float:
        with self._lock:
            if len(self._window_frames) < 2:
                return 0.0
            elapsed = self._window_frames[-1] - self._window_frames[0]
            if elapsed <= 0:
                return 0.0
            return sum(self._window_bytes) * 8 / elapsed / 1_000_000

    def reset(self) -> None:
        with self._lock:
            self.frame_count = 0
            self.byte_count = 0
            self.latency_ms = 0.0
            self.vcam_latency_ms = 0.0
            self._start = time.time()
            self._window_frames.clear()
            self._window_bytes.clear()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "name":         self.name,
                "connected":    self.connected,
                "frames":       self.frame_count,
                "codec":        self.codec,
                "resolution":   self.resolution,
                "fps":          self.fps,
                "mbps":         self.bitrate_mbps,
                "latency":      self.latency_ms,
                "vcam_latency": self.vcam_latency_ms,
            }


# ── Non-blocking key reader ───────────────────────────────────────────────────
class _KeyReader:
    """Read single keypresses without blocking or requiring Enter."""

    def __init__(self) -> None:
        self._old_settings = None
        self._active = False

    def __enter__(self) -> "_KeyReader":
        if sys.stdin.isatty():
            try:
                self._old_settings = termios.tcgetattr(sys.stdin.fileno())
                tty.setcbreak(sys.stdin.fileno())
                self._active = True
            except Exception:
                pass
        return self

    def __exit__(self, *_) -> None:
        if self._old_settings is not None:
            try:
                termios.tcsetattr(
                    sys.stdin.fileno(), termios.TCSADRAIN, self._old_settings
                )
            except Exception:
                pass

    def read(self) -> Optional[str]:
        """Return a pressed key or None (non-blocking)."""
        if not self._active:
            return None
        try:
            rlist, _, _ = select.select([sys.stdin], [], [], 0)
            if rlist:
                return sys.stdin.read(1)
        except Exception:
            pass
        return None


# ── Rich dashboard ────────────────────────────────────────────────────────────
def _build_table(
    ws_stats: _TransportStats,
    srt_stats: _TransportStats,
    local_ip: str,
    http_port: int,
    https_port: int,
    srt_port: int,
) -> "rich.table.Table":  # type: ignore[name-defined]
    from rich.table import Table
    from rich import box

    ws  = ws_stats.snapshot()
    srt = srt_stats.snapshot()

    # Show only the currently active transport (or the one that streamed last)
    if srt["connected"]:
        active = srt
        active_name = "SRT"
        active_color = "yellow"
    elif ws["connected"]:
        active = ws
        active_name = "WebSocket"
        active_color = "green"
    elif srt["frames"] > ws["frames"]:
        active = srt
        active_name = "SRT"
        active_color = "yellow"
    elif ws["frames"] > 0:
        active = ws
        active_name = "WebSocket"
        active_color = "green"
    else:
        active = ws
        active_name = "—"
        active_color = "dim"

    status_str = f"[{active_color}]● live[/{active_color}]" if active["connected"] else "[dim]○ idle[/dim]"
    transport_str = f"[{active_color}]{active_name}[/{active_color}]" if active_name != "—" else "[dim]Waiting for stream…[/dim]"

    fps_val = active["fps"]
    fps_str = f"{int(round(fps_val))}fps" if fps_val > 0 else "0fps"

    mbps_val = active["mbps"]
    if mbps_val >= 1.0:
        bitrate_str = f"{mbps_val:.1f} Mbps"
    elif mbps_val > 0.0:
        bitrate_str = f"{mbps_val * 1000:.0f} kbps"
    else:
        bitrate_str = "0.0 Mbps"

    t = Table(box=box.ROUNDED, show_header=False, padding=(0, 1), expand=True)
    t.add_column("Property", style="bold cyan", no_wrap=True, width=16, justify="left")
    t.add_column("Value", justify="left")

    t.add_row("Transport",  transport_str)
    t.add_row("Status",     status_str)
    t.add_row("Codec",      active["codec"])
    t.add_row("Resolution", active["resolution"])
    t.add_row("FPS",        fps_str)
    t.add_row("Bitrate",    bitrate_str)
    t.add_row(
        "Latency",
        f"{active['latency']:.1f} ms" if active["latency"] > 0 else "—",
    )
    t.add_row(
        "Frames",
        f"{active['frames']:,}",
    )

    return t


def _build_layout(
    ws_stats: _TransportStats,
    srt_stats: _TransportStats,
    local_ip: str,
    http_port: int,
    https_port: int,
    srt_port: int,
    preview_open: bool,
    virtual_camera: str = "",
    vcam_latency_ms: float = 0.0,
) -> "rich.console.RenderableType":  # type: ignore[name-defined]
    from rich.panel import Panel
    from rich.console import Group
    from rich.text import Text

    table = _build_table(ws_stats, srt_stats, local_ip, http_port, https_port, srt_port)

    urls = []
    if http_port:
        urls.append(f"[dim]HTTP [/dim]http://{local_ip}:{http_port}/")
    if https_port:
        urls.append(f"[dim]HTTPS[/dim] https://{local_ip}:{https_port}/")
    if srt_port:
        urls.append(f"[dim]SRT  [/dim]srt://{local_ip}:{srt_port}")
    if virtual_camera:
        cam_desc = virtual_camera if virtual_camera != "auto" else "Active (auto-detected)"
        lat_str = f" [cyan]({vcam_latency_ms:.1f} ms latency)[/cyan]" if vcam_latency_ms > 0 else ""
        urls.append(f"[dim]VCAM [/dim][magenta]{cam_desc}[/magenta]{lat_str}")
    url_text = "\n".join(urls)

    preview_hint = "[green]● preview open[/green]" if preview_open else ""
    shortcuts = (
        "[dim]preview[/dim] [bold](p)[/bold]  "
        "[dim]vcam[/dim] [bold](v)[/bold]  "
        "[dim]snapshot[/dim] [bold](s)[/bold]  "
        "[dim]reset[/dim] [bold](r)[/bold]  "
        "[dim]quit[/dim] [bold](q)[/bold]"
    )

    content_items = [table]
    if url_text:
        content_items.append(Text.from_markup(f"\n{url_text}\n"))
    hint_sep = "   " if preview_hint else ""
    content_items.append(Text.from_markup(f"{shortcuts}{hint_sep}{preview_hint}"))

    return Panel(
        Group(*content_items),
        title="[bold white]StreamRelay[/bold white]",
        border_style="cyan",
    )



# ── Standalone Viewer Components (based on standalone_viewer.py) ──────────────
class FPSCounter:
    """Rolling FPS counter using a fixed-size time window."""

    def __init__(self, window_size: int = 30):
        self._timestamps: deque = deque(maxlen=window_size)

    def tick(self) -> None:
        """Record a frame timestamp."""
        self._timestamps.append(time.perf_counter())

    @property
    def fps(self) -> float:
        """Calculate current FPS from recent timestamps."""
        if len(self._timestamps) < 2:
            return 0.0
        elapsed = self._timestamps[-1] - self._timestamps[0]
        if elapsed <= 0:
            return 0.0
        return (len(self._timestamps) - 1) / elapsed


def fit_frame_to_window(
    frame: np.ndarray,
    window_size: tuple[int, int],
    bg_color: tuple[int, int, int] = (30, 30, 30),
) -> np.ndarray:
    """Scale frame to fit within window while maintaining aspect ratio.

    Adds letterboxing (horizontal bars) or pillarboxing (vertical bars) as needed.
    """
    import cv2
    frame_h, frame_w = frame.shape[:2]
    win_w, win_h = window_size

    if win_w <= 0 or win_h <= 0:
        return frame

    scale = min(win_w / frame_w, win_h / frame_h)
    new_w = int(frame_w * scale)
    new_h = int(frame_h * scale)

    if scale != 1.0:
        resized = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
    else:
        resized = frame

    canvas = np.full((win_h, win_w, 3), bg_color, dtype=np.uint8)
    x_offset = (win_w - new_w) // 2
    y_offset = (win_h - new_h) // 2

    canvas[y_offset:y_offset + new_h, x_offset:x_offset + new_w] = resized
    return canvas


def draw_stats_overlay(
    frame: np.ndarray,
    fps: float,
    frame_count: int,
    resolution: tuple[int, int],
    latency_ms: float = 0.0,
) -> np.ndarray:
    """Draw FPS and stats overlay on the frame."""
    import cv2
    overlay = frame.copy()
    box_height = 115 if latency_ms > 0 else 90
    box_width = 210
    padding = 10

    cv2.rectangle(
        overlay,
        (padding, padding),
        (padding + box_width, padding + box_height),
        (0, 0, 0),
        -1,
    )

    alpha = 0.7
    frame = cv2.addWeighted(overlay, alpha, frame, 1 - alpha, 0)

    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.6
    thickness = 1
    text_color = (255, 255, 255)
    value_color = (0, 255, 128)

    y_offset = padding + 25
    line_height = 22

    cv2.putText(frame, "FPS:", (padding + 10, y_offset), font, font_scale, text_color, thickness)
    cv2.putText(frame, f"{fps:.1f}", (padding + 60, y_offset), font, font_scale, value_color, thickness)

    y_offset += line_height
    cv2.putText(frame, "Frames:", (padding + 10, y_offset), font, font_scale, text_color, thickness)
    cv2.putText(frame, f"{frame_count}", (padding + 85, y_offset), font, font_scale, value_color, thickness)

    y_offset += line_height
    cv2.putText(frame, "Size:", (padding + 10, y_offset), font, font_scale, text_color, thickness)
    cv2.putText(frame, f"{resolution[0]}x{resolution[1]}", (padding + 60, y_offset), font, font_scale, value_color, thickness)

    if latency_ms > 0:
        y_offset += line_height
        cv2.putText(frame, "Latency:", (padding + 10, y_offset), font, font_scale, text_color, thickness)
        cv2.putText(frame, f"{latency_ms:.1f} ms", (padding + 90, y_offset), font, font_scale, (0, 255, 255), thickness)

    return frame


def _serve_subprocess(server_kwargs: dict) -> None:
    """Run StreamServer in its own dedicated process."""
    import signal
    from .server import StreamServer
    server = StreamServer(**server_kwargs)
    def _sig(sig, frame):
        try:
            server.stop()
        except Exception:
            pass
        os._exit(0)
    try:
        signal.signal(signal.SIGINT, _sig)
        signal.signal(signal.SIGTERM, _sig)
    except Exception:
        pass
    server.run()


def run_preview_mode(
    shm_name: str,
    http_port: int,
    https_port: int,
    srt_port: int,
    host: str,
    cert_file: str,
    key_file: str,
    cert_dir: str,
    virtual_camera: str,
    snapshot_dir: Path,
) -> None:
    """Run StreamServer in a subprocess and display frames in an OpenCV window

    matching examples/standalone_viewer.py.
    """
    import multiprocessing as mp
    import cv2
    import signal

    server_kwargs = {
        "shm_name": shm_name,
        "http_port": http_port,
        "https_port": https_port,
        "srt_port": srt_port,
        "host": host,
        "cert_file": cert_file,
        "key_file": key_file,
        "cert_dir": cert_dir,
        "virtual_camera": virtual_camera,
    }

    print("[viewer] Starting StreamRelay server subprocess...")
    server_proc = mp.Process(target=_serve_subprocess, args=(server_kwargs,), daemon=True)
    server_proc.start()

    def _cleanup_and_exit(sig=None, frame=None):
        print("\n[viewer] Exiting...")
        try:
            cv2.destroyAllWindows()
            for _ in range(3):
                cv2.waitKey(1)
        except Exception:
            pass
        if server_proc.is_alive():
            server_proc.terminate()
            server_proc.join(timeout=1.0)
            if server_proc.is_alive():
                server_proc.kill()
        os._exit(0)

    try:
        signal.signal(signal.SIGINT, _cleanup_and_exit)
        signal.signal(signal.SIGTERM, _cleanup_and_exit)
    except Exception:
        pass

    print(f"[viewer] Waiting for shared memory '{shm_name}' to initialize...")
    try:
        reader = FrameReader(shm_name=shm_name, attach_timeout=15.0)
    except FileNotFoundError as e:
        print(f"[viewer] Error: {e}")
        _cleanup_and_exit()
        return

    local_ip = _get_local_ip()
    print("[viewer] Server ready!")
    if https_port:
        print(f"[viewer] HTTPS: https://{local_ip}:{https_port}/")
    if http_port:
        print(f"[viewer] HTTP:  http://{local_ip}:{http_port}/")
    if srt_port:
        print(f"[viewer] SRT:   srt://{local_ip}:{srt_port}")
    print("[viewer] Controls: [ESC] or [q] to exit, [s] for snapshot.")
    print()

    fps_counter = FPSCounter(window_size=30)
    frame_count = 0
    last_frame = None
    window_size = (640, 480)
    WINDOW_NAME = "StreamRelay Viewer"
    snapshot_counter = 0

    cv2.namedWindow(WINDOW_NAME, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(WINDOW_NAME, window_size[0], window_size[1])

    # Show initial waiting canvas
    init_canvas = np.full((window_size[1], window_size[0], 3), (30, 30, 30), dtype=np.uint8)
    text = "Waiting for video stream..."
    font = cv2.FONT_HERSHEY_SIMPLEX
    (tw, th), _ = cv2.getTextSize(text, font, 0.7, 2)
    cx = (window_size[0] - tw) // 2
    cy = (window_size[1] + th) // 2
    cv2.putText(init_canvas, text, (cx, cy), font, 0.7, (180, 180, 180), 2, cv2.LINE_AA)
    cv2.imshow(WINDOW_NAME, init_canvas)
    cv2.waitKey(1)

    try:
        while True:
            result = reader.read_new_with_info()

            if result is not None:
                frame, info = result
                fps_counter.tick()
                frame_count += 1
                last_frame = frame
                resolution = (info.width, info.height)

                frame_with_stats = draw_stats_overlay(
                    frame.copy(),
                    fps_counter.fps,
                    frame_count,
                    resolution,
                )

                try:
                    win_rect = cv2.getWindowImageRect(WINDOW_NAME)
                    win_w = int(win_rect[2])
                    win_h = int(win_rect[3])
                    if win_w > 0 and win_h > 0:
                        window_size = (win_w, win_h)
                except Exception:
                    pass

                display_frame = fit_frame_to_window(frame_with_stats, window_size)
                cv2.imshow(WINDOW_NAME, display_frame)

            elif last_frame is not None:
                h, w = last_frame.shape[:2]
                frame_with_stats = draw_stats_overlay(
                    last_frame.copy(),
                    fps_counter.fps,
                    frame_count,
                    (w, h),
                )

                try:
                    win_rect = cv2.getWindowImageRect(WINDOW_NAME)
                    win_w = int(win_rect[2])
                    win_h = int(win_rect[3])
                    if win_w > 0 and win_h > 0:
                        window_size = (win_w, win_h)
                except Exception:
                    pass

                display_frame = fit_frame_to_window(frame_with_stats, window_size)
                cv2.imshow(WINDOW_NAME, display_frame)

            key = cv2.waitKey(1) & 0xFF
            if key in (27, ord('q')):  # ESC or q
                break
            elif key == ord('s') and last_frame is not None:
                snapshot_dir.mkdir(parents=True, exist_ok=True)
                snapshot_counter += 1
                path = snapshot_dir / f"frame_{snapshot_counter:04d}.jpg"
                cv2.imwrite(str(path), last_frame)
                print(f"[viewer] Snapshot saved: {path}")

            try:
                # Check if window was closed with red OS button (X)
                if cv2.getWindowProperty(WINDOW_NAME, cv2.WND_PROP_VISIBLE) < 1:
                    break
            except Exception:
                break

            if result is None:
                time.sleep(0.005)

    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        print(f"[viewer] Received {frame_count} frames total.")
        try:
            reader.close()
        except Exception:
            pass
        _cleanup_and_exit()


# ── Preview window ────────────────────────────────────────────────────────────
class _PreviewWindow:
    """Thin wrapper around an OpenCV named window."""

    WINDOW = "StreamRelay Preview"

    def __init__(self) -> None:
        self._open = False
        self._last_frame: Optional[np.ndarray] = None
        self._last_latency = 0.0
        self._last_res: Optional[tuple[int, int]] = None
        self._has_new_frame = False
        self._lock = threading.Lock()
        self._snapshot_counter = 0
        self._fps_counter = FPSCounter()
        self._frame_count = 0

    def _make_placeholder(self) -> np.ndarray:
        import cv2
        img = np.zeros((480, 854, 3), dtype=np.uint8)
        img[:] = (32, 28, 28)
        text = "StreamRelay — Waiting for video stream..."
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.7
        thickness = 2
        (tw, th), _ = cv2.getTextSize(text, font, font_scale, thickness)
        cx = (854 - tw) // 2
        cy = (480 + th) // 2
        cv2.putText(
            img, text, (cx, cy), font, font_scale, (180, 180, 180), thickness, cv2.LINE_AA
        )
        return img

    def toggle(self) -> None:
        if self._open:
            self.close()
        else:
            self.open()

    def open(self) -> None:
        import cv2
        cv2.namedWindow(self.WINDOW, cv2.WINDOW_NORMAL)
        with self._lock:
            frame = self._last_frame
        if frame is not None:
            fh, fw = frame.shape[:2]
            max_w, max_h = 960, 640
            scale = min(max_w / max(1, fw), max_h / max(1, fh), 1.0)
            win_w = max(320, int(fw * scale))
            win_h = max(240, int(fh * scale))
            cv2.resizeWindow(self.WINDOW, win_w, win_h)
            display_frame = frame
        else:
            cv2.resizeWindow(self.WINDOW, 854, 480)
            display_frame = self._make_placeholder()
        cv2.imshow(self.WINDOW, display_frame)
        cv2.waitKey(1)
        self._open = True

    def close(self) -> None:
        import cv2
        try:
            cv2.destroyWindow(self.WINDOW)
            for _ in range(3):
                cv2.waitKey(1)
        except Exception:
            pass
        self._open = False
        self._last_res = None

    def push_frame(self, frame: np.ndarray, latency_ms: float = 0.0) -> None:
        with self._lock:
            self._last_frame = frame
            self._last_latency = latency_ms
            self._has_new_frame = True

    def tick(self) -> None:
        """Call from main loop to flush frame to screen."""
        if not self._open:
            return
        import cv2
        frame = None
        latency = 0.0
        with self._lock:
            if self._has_new_frame:
                frame = self._last_frame
                latency = self._last_latency
                self._has_new_frame = False

        if frame is not None:
            self._fps_counter.tick()
            self._frame_count += 1
            h, w = frame.shape[:2]

            # Auto-adapt window size and aspect ratio when stream resolution changes
            if self._last_res != (w, h):
                self._last_res = (w, h)
                max_w, max_h = 960, 640
                scale = min(max_w / max(1, w), max_h / max(1, h), 1.0)
                win_w = max(320, int(w * scale))
                win_h = max(240, int(h * scale))
                try:
                    cv2.resizeWindow(self.WINDOW, win_w, win_h)
                except Exception:
                    pass

            display = draw_stats_overlay(
                frame.copy(),
                self._fps_counter.fps,
                self._frame_count,
                (w, h),
                latency,
            )

            cv2.imshow(self.WINDOW, display)
        key = cv2.waitKey(1) & 0xFF
        if key in (27, ord('q'), ord('p')):  # ESC, q, or p inside the cv2 window closes preview
            self.close()
            return None
        elif key == ord('v'):
            return 'v'
        elif key == ord('s'):
            return 's'
        elif key == ord('r'):
            return 'r'
        else:
            try:
                # On macOS Cocoa, clicking the OS close button (X) destroys the NSWindow
                # causing cv2.getWindowProperty to return < 0 or throw cv2.error
                prop = cv2.getWindowProperty(self.WINDOW, cv2.WND_PROP_VISIBLE)
                if prop < 0:
                    self.close()
            except cv2.error:
                self.close()
            except Exception:
                pass
        return None

    def snapshot(self, out_dir: Path) -> Optional[Path]:
        import cv2
        with self._lock:
            frame = self._last_frame
        if frame is None:
            return None
        out_dir.mkdir(parents=True, exist_ok=True)
        self._snapshot_counter += 1
        path = out_dir / f"frame_{self._snapshot_counter:04d}.jpg"
        cv2.imwrite(str(path), frame)
        return path

    @property
    def is_open(self) -> bool:
        return self._open


# ── Main interactive CLI ──────────────────────────────────────────────────────
def run_interactive_cli(
    shm_name: str,
    http_port: int,
    https_port: int,
    srt_port: int,
    host: str,
    cert_file: str,
    key_file: str,
    cert_dir: str,
    virtual_camera: str,
    preview: bool,
    snapshot_dir: Path,
) -> None:
    """Start the server + rich interactive dashboard."""
    try:
        from rich.live import Live
        from rich.console import Console
        has_rich = True
    except ImportError:
        has_rich = False

    ws_stats  = _TransportStats("WebSocket")
    srt_stats = _TransportStats("SRT")

    # Intercept frames from each transport to update stats
    def _on_frame_ws(frame: np.ndarray) -> None:
        ws_stats.record_frame(frame)
        preview_win.push_frame(frame)

    def _on_frame_srt(frame: np.ndarray) -> None:
        srt_stats.record_frame(frame)
        preview_win.push_frame(frame)

    def _on_frame_any(frame: np.ndarray) -> None:
        """Single on_frame path when SRT shares the server's dispatch."""
        # SRT frames arrive here via SRTListener.on_frame = server._dispatch_frame.
        # We hook at the StreamServer level instead (see on_frame= below).
        pass

    preview_win = _PreviewWindow()
    if preview:
        preview_win.open()

    # Resolve the machine's LAN IP for display
    import socket as _socket
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = "127.0.0.1"

    # Combined on_frame that updates stats AND forwards to preview
    def _combined_on_frame(
        frame: np.ndarray,
        latency_ms: float = 0.0,
        source: str = "ws",
        codec: str = "—",
        byte_size: Optional[int] = None,
    ) -> None:
        vcam_lat = (
            server._virtual_cam.latest_latency_ms
            if server._virtual_cam
            else 0.0
        )
        if source == "srt":
            srt_stats.record_frame(frame, latency_ms, vcam_lat, codec="H.264", byte_size=byte_size)
        else:
            ws_stats.record_frame(frame, latency_ms, vcam_lat, codec=codec, byte_size=byte_size)
        preview_win.push_frame(frame, latency_ms)

    server = StreamServer(
        shm_name=shm_name,
        http_port=http_port,
        https_port=https_port,
        srt_port=srt_port,
        host=host,
        cert_file=cert_file,
        key_file=key_file,
        cert_dir=cert_dir,
        virtual_camera=virtual_camera,
        on_frame=_combined_on_frame,
    )

    import signal

    shutdown_event = threading.Event()

    def _on_signal(sig, frame):
        shutdown_event.set()
        try:
            preview_win.close()
            server.stop()
        except Exception:
            pass
        os._exit(0)

    try:
        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)
    except Exception:
        pass

    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.5)   # let server print its startup lines first

    if not has_rich:
        print("[streamrelay] pip install rich for the interactive dashboard")
        print("[streamrelay] Running in plain log mode. Ctrl-C to quit.")
        try:
            while not shutdown_event.is_set():
                time.sleep(0.2)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            server.stop()
        return

    from rich.live import Live
    from rich.console import Console

    console = Console()

    def _render():
        vcam_lat = server._virtual_cam.latest_latency_ms if server._virtual_cam else 0.0
        return _build_layout(
            ws_stats, srt_stats, local_ip,
            http_port, https_port, srt_port,
            preview_win.is_open,
            server.virtual_camera,
            vcam_latency_ms=vcam_lat,
        )

    try:
        last_render_time = 0.0
        with _KeyReader() as keys, Live(
            _render(), console=console, refresh_per_second=4, screen=False
        ) as live:
            while not shutdown_event.is_set():
                key = keys.read()
                win_key = preview_win.tick()
                active_key = key or win_key
                if active_key:
                    k = active_key.lower() if isinstance(active_key, str) else chr(active_key).lower()
                    if k in ("q", "\x03", "\x04"):   # q, Ctrl-C, Ctrl-D
                        break
                    elif k == "p":
                        preview_win.toggle()
                    elif k == "v":
                        enabled, msg = server.toggle_virtual_camera()
                        if enabled:
                            console.print(f"[green]● {msg}[/green]")
                        else:
                            console.print(f"[yellow]○ {msg}[/yellow]")
                        if hasattr(live, "_live_render"):
                            live._live_render._shape = None
                        live.update(_render(), refresh=True)
                    elif k == "s":
                        path = preview_win.snapshot(snapshot_dir)
                        if path:
                            console.print(f"[green]Snapshot saved: {path}[/green]")
                        else:
                            console.print("[yellow]No frame available yet[/yellow]")
                    elif k == "r":
                        ws_stats.reset()
                        srt_stats.reset()
                        if server._virtual_cam is not None:
                            server._virtual_cam.reset()
                        console.clear()
                        if hasattr(live, "_live_render"):
                            live._live_render._shape = None
                        live.update(_render(), refresh=True)

                now = time.monotonic()
                if now - last_render_time >= 0.25:
                    live.update(_render())
                    last_render_time = now

                if not preview_win.is_open:
                    time.sleep(0.02)
                else:
                    time.sleep(0.001)

    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        preview_win.close()
        server.stop()
        import cv2
        try:
            cv2.destroyAllWindows()
            for _ in range(3):
                cv2.waitKey(1)
        except Exception:
            pass
        os._exit(0)


# ── CLI entry point ───────────────────────────────────────────────────────────
def main() -> None:
    """Console-script entry point: ``streamrelay-server``."""
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
                        help="Directory for auto-generated certs")
    parser.add_argument(
        "--virtual-camera", nargs="?", const="auto", default="", metavar="DEVICE",
        help="Enable virtual camera output (auto, OBS on macOS, or /dev/video10 on Linux)",
    )
    parser.add_argument(
        "--no-stats", action="store_true",
        help="Disable rich live stats (plain log output)",
    )
    parser.add_argument(
        "--preview", action="store_true",
        help="Launch OpenCV preview window immediately on start",
    )
    parser.add_argument(
        "--snapshot-dir", default="./snapshots",
        help="Directory where [s] key saves snapshots",
    )
    args = parser.parse_args()

    try:
        if args.no_stats:
            # Plain mode: just run the server with no UI
            server = StreamServer(
                shm_name=args.shm_name,
                http_port=args.http_port,
                https_port=args.https_port,
                srt_port=args.srt_port,
                host=args.host,
                cert_file=args.cert_file,
                key_file=args.key_file,
                cert_dir=args.cert_dir,
                virtual_camera=args.virtual_camera,
            )
            import signal
            def _sig(sig, frame):
                server.stop()
                os._exit(0)
            try:
                signal.signal(signal.SIGINT, _sig)
                signal.signal(signal.SIGTERM, _sig)
            except Exception:
                pass
            server.run()
        else:
            run_interactive_cli(
                shm_name=args.shm_name,
                http_port=args.http_port,
                https_port=args.https_port,
                srt_port=args.srt_port,
                host=args.host,
                cert_file=args.cert_file,
                key_file=args.key_file,
                cert_dir=args.cert_dir,
                virtual_camera=args.virtual_camera,
                preview=args.preview,
                snapshot_dir=Path(args.snapshot_dir),
            )
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        os._exit(0)

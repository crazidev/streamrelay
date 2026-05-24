"""Standalone streamrelay viewer with FPS display.

Run the streamrelay server and display received frames in an OpenCV window
with real-time FPS statistics overlay.

Usage:
    python examples/standalone_viewer.py

Then point your phone's browser at https://<this-host>:9090/
Press ESC to exit.
"""

from __future__ import annotations

import multiprocessing as mp
import time
from collections import deque

import cv2
import numpy as np

from streamrelay import FrameReader, StreamServer

SHM_NAME = "streamrelay_demo"
WINDOW_NAME = "StreamRelay Viewer"


def _serve():
    """Run the StreamServer in a subprocess."""
    StreamServer(shm_name=SHM_NAME).run()


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
    frame_h, frame_w = frame.shape[:2]
    win_w, win_h = window_size
    
    if win_w <= 0 or win_h <= 0:
        return frame
    
    # Calculate scale to fit frame within window
    scale = min(win_w / frame_w, win_h / frame_h)
    
    # New dimensions
    new_w = int(frame_w * scale)
    new_h = int(frame_h * scale)
    
    # Resize frame
    if scale != 1.0:
        resized = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
    else:
        resized = frame
    
    # Create canvas with background color
    canvas = np.full((win_h, win_w, 3), bg_color, dtype=np.uint8)
    
    # Center the frame on canvas
    x_offset = (win_w - new_w) // 2
    y_offset = (win_h - new_h) // 2
    
    canvas[y_offset:y_offset + new_h, x_offset:x_offset + new_w] = resized
    
    return canvas


def draw_stats_overlay(
    frame: np.ndarray,
    fps: float,
    frame_count: int,
    resolution: tuple[int, int],
) -> np.ndarray:
    """Draw FPS and stats overlay on the frame."""
    # Create semi-transparent overlay background
    overlay = frame.copy()
    
    # Stats box dimensions
    box_height = 90
    box_width = 200
    padding = 10
    
    # Draw rectangle background
    cv2.rectangle(
        overlay,
        (padding, padding),
        (padding + box_width, padding + box_height),
        (0, 0, 0),
        -1,
    )
    
    # Blend overlay with original frame
    alpha = 0.7
    frame = cv2.addWeighted(overlay, alpha, frame, 1 - alpha, 0)
    
    # Text settings
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.6
    thickness = 1
    text_color = (255, 255, 255)
    value_color = (0, 255, 128)  # Green for values
    
    # Draw stats
    y_offset = padding + 25
    line_height = 22
    
    # FPS
    cv2.putText(frame, "FPS:", (padding + 10, y_offset), font, font_scale, text_color, thickness)
    cv2.putText(frame, f"{fps:.1f}", (padding + 60, y_offset), font, font_scale, value_color, thickness)
    
    # Frame count
    y_offset += line_height
    cv2.putText(frame, "Frames:", (padding + 10, y_offset), font, font_scale, text_color, thickness)
    cv2.putText(frame, f"{frame_count}", (padding + 80, y_offset), font, font_scale, value_color, thickness)
    
    # Resolution
    y_offset += line_height
    cv2.putText(frame, "Size:", (padding + 10, y_offset), font, font_scale, text_color, thickness)
    cv2.putText(frame, f"{resolution[0]}x{resolution[1]}", (padding + 60, y_offset), font, font_scale, value_color, thickness)
    
    return frame


def main():
    """Main entry point."""
    print("[viewer] Starting StreamRelay server...")
    server = mp.Process(target=_serve, daemon=True)
    server.start()

    print("[viewer] Waiting for server to initialize...")
    try:
        reader = FrameReader(shm_name=SHM_NAME, attach_timeout=15.0)
    except FileNotFoundError as e:
        print(f"[viewer] Error: {e}")
        return

    print("[viewer] Server ready!")
    print("[viewer] Open https://<this-host>:9090/ on your phone or browser.")
    print("[viewer] Press ESC to exit.")
    print()

    fps_counter = FPSCounter(window_size=30)
    frame_count = 0
    last_frame = None
    last_resolution = (0, 0)
    window_size = (480, 640)  # Default window size
    
    # Create fixed-size window - frame will be scaled to fit inside
    cv2.namedWindow(WINDOW_NAME, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(WINDOW_NAME, window_size[0], window_size[1])

    try:
        while True:
            result = reader.read_new_with_info()
            
            if result is not None:
                frame, info = result
                fps_counter.tick()
                frame_count += 1
                last_frame = frame
                resolution = (info.width, info.height)
                last_resolution = resolution
                
                # Draw stats overlay on original frame
                frame_with_stats = draw_stats_overlay(
                    frame.copy(),
                    fps_counter.fps,
                    frame_count,
                    resolution,
                )
                
                # Get current window size and fit frame to it
                try:
                    win_w = int(cv2.getWindowImageRect(WINDOW_NAME)[2])
                    win_h = int(cv2.getWindowImageRect(WINDOW_NAME)[3])
                    if win_w > 0 and win_h > 0:
                        window_size = (win_w, win_h)
                except:
                    pass
                
                display_frame = fit_frame_to_window(frame_with_stats, window_size)
                cv2.imshow(WINDOW_NAME, display_frame)
                
            elif last_frame is not None:
                # No new frame, but we have a previous frame to show
                h, w = last_frame.shape[:2]
                frame_with_stats = draw_stats_overlay(
                    last_frame.copy(),
                    fps_counter.fps,
                    frame_count,
                    (w, h),
                )
                
                # Get current window size
                try:
                    win_w = int(cv2.getWindowImageRect(WINDOW_NAME)[2])
                    win_h = int(cv2.getWindowImageRect(WINDOW_NAME)[3])
                    if win_w > 0 and win_h > 0:
                        window_size = (win_w, win_h)
                except:
                    pass
                
                display_frame = fit_frame_to_window(frame_with_stats, window_size)
                cv2.imshow(WINDOW_NAME, display_frame)
            
            # Check for ESC key (27) or window close
            key = cv2.waitKey(1) & 0xFF
            if key == 27:  # ESC
                break
            
            # Check if window was closed
            if cv2.getWindowProperty(WINDOW_NAME, cv2.WND_PROP_VISIBLE) < 1:
                break
                
            # Small sleep to prevent busy-waiting when no frames
            if result is None:
                time.sleep(0.005)
                
    except KeyboardInterrupt:
        print("\n[viewer] Interrupted by user.")
    finally:
        print(f"[viewer] Received {frame_count} frames total.")
        cv2.destroyAllWindows()
        reader.close()


if __name__ == "__main__":
    main()

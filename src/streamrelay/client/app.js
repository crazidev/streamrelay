/* StreamRelay — Multi-transport streaming client
 * Transport: Auto (WS H.264 → WS JPEG) | WS H.264 | WS JPEG
 *
 * Auto mode tries each transport in priority order and falls back silently.
 * The dropdown updates to show which transport was actually used.
 * Explicit modes skip fallback and show an error if the chosen mode fails.
 */
'use strict';

// ── DOM refs ──────────────────────────────────────────────────────────────────
const localVideo       = document.getElementById('localVideo');
const videoOverlay     = document.getElementById('videoOverlay');
const streamBtn        = document.getElementById('streamBtn');
const streamBtnIcon    = document.getElementById('streamBtnIcon');
const streamBtnText    = document.getElementById('streamBtnText');
const cameraSelect     = document.getElementById('cameraSelect');
const resolutionSelect = document.getElementById('resolutionSelect');
const transportSelect  = document.getElementById('transportSelect');
const qualityRange     = document.getElementById('qualityRange');
const qualityLabel     = document.getElementById('qualityLabel');
const qualityRow       = document.getElementById('qualityRow');
const statusDot        = document.getElementById('statusDot');
const panelToggle      = document.getElementById('panelToggle');
const panelBody        = document.getElementById('panelBody');
const themeBtn         = document.getElementById('themeBtn');
const statFps          = document.getElementById('statFps');
const statBitrate      = document.getElementById('statBitrate');
const statResolution   = document.getElementById('statResolution');
const statCodec        = document.getElementById('statCodec');
const capabilityRow    = document.getElementById('capabilityRow');
const capBadge         = document.getElementById('capBadge');

// ── State ─────────────────────────────────────────────────────────────────────
let localStream  = null;
let isStreaming  = false;
/** @type {'ws-h264'|'ws-jpeg'} Active transport actually in use */
let activeMode   = null;

// WebSocket
let ws           = null;
let captureLoop  = null;
let hiddenVideo  = null;
let encoder      = null;
let sendCanvas   = null;
let sendCtx      = null;
let mirrorCanvas = null;
let mirrorCtx    = null;

// Stats
let statsInterval   = null;
let framesSent      = 0;
let bytesSent       = 0;
let lastStatsTime   = 0;
let lastStatsFrames = 0;
let lastStatsBytes  = 0;

const TARGET_FPS = 30;

// ── Icons ─────────────────────────────────────────────────────────────────────
const playIcon = `<svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor"><polygon points="5,3 19,12 5,21"/></svg>`;
const stopIcon = `<svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="3"/></svg>`;

// ── Theme ─────────────────────────────────────────────────────────────────────
function applyTheme(t) { document.documentElement.setAttribute('data-theme', t); localStorage.setItem('streamrelay_theme', t); }
themeBtn.addEventListener('click', () => applyTheme(document.documentElement.getAttribute('data-theme') === 'light' ? 'dark' : 'light'));
applyTheme(localStorage.getItem('streamrelay_theme') || 'dark');

// ── Panel collapse ────────────────────────────────────────────────────────────
panelToggle.addEventListener('click', () => { const c = panelBody.classList.toggle('collapsed'); panelToggle.classList.toggle('collapsed', c); });

// ── Status helpers ────────────────────────────────────────────────────────────
function setStatus(s) { statusDot.className = 'status-dot ' + s; }

function setStreamBtn(state) {
  if (state === 'streaming') {
    isStreaming = true;
    streamBtn.classList.add('streaming'); streamBtn.classList.remove('connecting');
    streamBtnIcon.innerHTML = stopIcon; streamBtnText.textContent = 'Stop Streaming'; streamBtn.disabled = false;
  } else if (state === 'connecting') {
    isStreaming = false;
    streamBtn.classList.remove('streaming'); streamBtn.classList.add('connecting');
    streamBtnIcon.innerHTML = playIcon; streamBtnText.textContent = 'Connecting...'; streamBtn.disabled = true;
  } else {
    isStreaming = false;
    streamBtn.classList.remove('streaming', 'connecting');
    streamBtnIcon.innerHTML = playIcon; streamBtnText.textContent = 'Start Streaming'; streamBtn.disabled = false;
  }
}

const MODE_LABELS = { 'ws-h264': 'H.264', 'ws-jpeg': 'JPEG' };
const MODE_CSS    = { 'ws-h264': 'stat-codec--h264', 'ws-jpeg': 'stat-codec--jpeg' };

/**
 * Called once a transport is confirmed active.
 * Updates the stats badge and — if auto was selected — updates the dropdown
 * to show which transport was actually used.
 */
function onTransportActive(mode) {
  activeMode = mode;
  statCodec.textContent = MODE_LABELS[mode] || mode;
  statCodec.className = 'stat-value stat-codec ' + (MODE_CSS[mode] || '');

  const userChoice = transportSelect.value;
  if (userChoice === 'auto') {
    // Show badge indicating what auto picked, but keep dropdown on "Auto"
    capabilityRow.style.display = '';
    capBadge.textContent = `Auto selected: ${MODE_LABELS[mode]}`;
    capBadge.className = 'cap-badge cap-badge--' + mode;
  } else {
    capabilityRow.style.display = 'none';
  }
}

// Show/hide quality slider based on dropdown selection
function onTransportChange() {
  const v = transportSelect.value;
  qualityRow.style.display = (v === 'ws-jpeg') ? '' : 'none';
  capabilityRow.style.display = 'none';
  saveSettings();
}
transportSelect.addEventListener('change', onTransportChange);

// ── Capability detection ──────────────────────────────────────────────────────
function webCodecsSupported() { return typeof VideoEncoder !== 'undefined' && typeof VideoFrame !== 'undefined'; }

// ── Stream button ─────────────────────────────────────────────────────────────
streamBtn.addEventListener('click', () => { if (isStreaming) cleanUp(); else startStreaming(); });

// ── Stats ─────────────────────────────────────────────────────────────────────
function startStats() {
  lastStatsTime = performance.now(); lastStatsFrames = 0; lastStatsBytes = 0;
  statsInterval = setInterval(() => {
    const now = performance.now(), elapsed = (now - lastStatsTime) / 1000;
    if (elapsed < 0.5) return;
    statFps.textContent = String(Math.round((framesSent - lastStatsFrames) / elapsed));
    const kbps = Math.round(((bytesSent - lastStatsBytes) * 8) / elapsed / 1000);
    statBitrate.textContent = kbps > 1000 ? (kbps / 1000).toFixed(1) + ' Mbps' : kbps + ' kbps';
    lastStatsTime = now; lastStatsFrames = framesSent; lastStatsBytes = bytesSent;
  }, 1000);
}

function stopStats() {
  if (statsInterval) { clearInterval(statsInterval); statsInterval = null; }
  statFps.textContent = '0'; statBitrate.textContent = '0 kbps';
  statResolution.textContent = '—'; statCodec.textContent = '—';
  statCodec.className = 'stat-value stat-codec';
}

// ── Cameras ───────────────────────────────────────────────────────────────────
function fmtCam(label, i) {
  if (!label) return `Camera ${i + 1}`;
  const c = label.replace(/\s*\([0-9a-f]{4}:[0-9a-f]{4}\)/gi, '').trim();
  return (c.length > 32 ? c.slice(0, 30) + '…' : c) || `Camera ${i + 1}`;
}
async function enumerateCameras() {
  try { const t = await navigator.mediaDevices.getUserMedia({video:true}); t.getTracks().forEach(t=>t.stop()); } catch(_){}
  const cams = (await navigator.mediaDevices.enumerateDevices()).filter(d => d.kind === 'videoinput');
  cameraSelect.innerHTML = '';
  if (!cams.length) { cameraSelect.innerHTML = '<option>No cameras found</option>'; return; }
  cams.forEach((c, i) => { const o = document.createElement('option'); o.value = c.deviceId; o.textContent = fmtCam(c.label, i); cameraSelect.appendChild(o); });
}
function getConstraints() {
  const [w, h] = resolutionSelect.value.split('x').map(Number);
  return { video: { deviceId: cameraSelect.value ? { exact: cameraSelect.value } : undefined, frameRate: { ideal: TARGET_FPS }, width: { ideal: w }, height: { ideal: h } }, audio: false };
}

// ── Settings persistence ──────────────────────────────────────────────────────
const SK = 'streamrelay_settings';
function getQuality() { return parseInt(qualityRange.value, 10) / 100; }
function setQuality(q) { const p = Math.round(q * 100); qualityRange.value = String(p); qualityLabel.textContent = p + '%'; }
qualityRange.addEventListener('input', () => { qualityLabel.textContent = qualityRange.value + '%'; });
qualityRange.addEventListener('change', saveSettings);
cameraSelect.addEventListener('change', saveSettings);
resolutionSelect.addEventListener('change', saveSettings);
function saveSettings() {
  localStorage.setItem(SK, JSON.stringify({ transport: transportSelect.value, cameraId: cameraSelect.value, cameraLabel: cameraSelect.selectedOptions[0]?.text ?? '', resolution: resolutionSelect.value, quality: qualityRange.value }));
}
function restoreSettings() {
  try {
    const s = JSON.parse(localStorage.getItem(SK) || '{}');
    if (s.transport) { const o = Array.from(transportSelect.options).find(o => o.value === s.transport); if (o) { transportSelect.value = s.transport; onTransportChange(); } }
    if (s.resolution) { const o = Array.from(resolutionSelect.options).find(o => o.value === s.resolution); if (o) resolutionSelect.value = s.resolution; }
    if (s.quality) setQuality(parseInt(s.quality, 10) / 100);
  } catch (_) {}
}
function restoreCameraSelection() {
  try {
    const s = JSON.parse(localStorage.getItem(SK) || '{}');
    const m = (s.cameraId && Array.from(cameraSelect.options).find(o => o.value === s.cameraId)) || (s.cameraLabel && Array.from(cameraSelect.options).find(o => o.text === s.cameraLabel));
    if (m) cameraSelect.value = m.value;
  } catch (_) {}
}

// ── Main entry ────────────────────────────────────────────────────────────────
async function startStreaming() {
  setStatus('connecting'); setStreamBtn('connecting');
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    alert('Camera access unavailable. Mobile browsers require HTTPS for camera. Please connect via https:// (e.g. https://IP:9090/) and accept the certificate.');
    setStatus('error'); setStreamBtn('idle'); return;
  }
  try {
    localStream = await navigator.mediaDevices.getUserMedia(getConstraints());
  } catch (err) {
    console.error('[Stream] Camera error:', err);
    alert('Camera error: ' + err.name + ' - ' + err.message + '\nPlease check camera permissions.');
    setStatus('error'); setStreamBtn('idle'); return;
  }
  const track = localStream.getVideoTracks()[0];
  const s = track.getSettings();
  statResolution.textContent = `${s.width || 1280}×${s.height || 720}`;

  const choice = transportSelect.value;

  if (choice === 'auto') {
    await startAuto();
  } else if (choice === 'ws-h264') {
    await startWebSocket('h264', /*allowFallback=*/false);
  } else {
    await startWebSocket('jpeg', false);
  }
}

/**
 * Auto mode: try WS H.264 → WS JPEG in order.
 * Each failure is silent; the next is tried automatically.
 */
async function startAuto() {
  // 1. WS H.264
  if (webCodecsSupported()) {
    console.log('[Auto] Trying WS H.264...');
    const ok = await tryWebSocket('h264');
    if (ok) return;
    console.log('[Auto] WS H.264 failed, falling back to WS JPEG...');
  }
  // 2. WS JPEG (always works)
  console.log('[Auto] Using WS JPEG');
  await startWebSocket('jpeg', false);
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
/** @returns {Promise<boolean>} true if connected (for auto mode) */
async function tryWebSocket(codec) {
  return new Promise(resolve => {
    startWebSocket(codec, true, resolve);
  });
}

async function startWebSocket(codec, reportResult = false, onResult = null) {
  hiddenVideo = document.createElement('video');
  hiddenVideo.srcObject = localStream; hiddenVideo.muted = true; hiddenVideo.playsInline = true;
  await hiddenVideo.play();
  if (!hiddenVideo.videoWidth) await new Promise(r => { const fn = () => { hiddenVideo.removeEventListener('loadedmetadata', fn); r(); }; hiddenVideo.addEventListener('loadedmetadata', fn); });

  const nW = hiddenVideo.videoWidth || 1280, nH = hiddenVideo.videoHeight || 720;
  const [selW, selH] = resolutionSelect.value.split('x').map(Number);
  const maxDim = Math.max(selW, selH);
  let sW = nW, sH = nH;
  if (Math.max(nW, nH) > maxDim) { const sc = maxDim / Math.max(nW, nH); sW = Math.round(nW * sc); sH = Math.round(nH * sc); }
  sW = Math.max(16, Math.round(sW / 16) * 16);
  sH = Math.max(16, Math.round(sH / 2) * 2);
  statResolution.textContent = `${sW}×${sH}`;

  mirrorCanvas = document.createElement('canvas'); mirrorCanvas.width = sW; mirrorCanvas.height = sH; mirrorCtx = mirrorCanvas.getContext('2d');
  sendCanvas = document.createElement('canvas'); sendCanvas.width = sW; sendCanvas.height = sH; sendCtx = sendCanvas.getContext('2d', { willReadFrequently: false });
  localVideo.srcObject = mirrorCanvas.captureStream(TARGET_FPS);
  localVideo.classList.add('visible'); videoOverlay.classList.add('hidden');

  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);
  ws.binaryType = 'arraybuffer';

  ws.onopen = async () => {
    framesSent = 0; bytesSent = 0;
    let actualCodec = codec;
    if (codec === 'h264') {
      try {
        await initH264Encoder(sW, sH);
        ws.send(JSON.stringify({ type: 'codec', codec: 'h264', width: sW, height: sH }));
      } catch (e) {
        console.warn('[WS] H.264 init failed:', e.message);
        if (reportResult) { cleanUpWS(); if (onResult) onResult(false); return; }
        actualCodec = 'jpeg';
        ws.send(JSON.stringify({ type: 'codec', codec: 'jpeg', width: sW, height: sH }));
      }
    } else {
      ws.send(JSON.stringify({ type: 'codec', codec: 'jpeg', width: sW, height: sH }));
    }
    onTransportActive(actualCodec === 'h264' ? 'ws-h264' : 'ws-jpeg');
    setStatus('active'); setStreamBtn('streaming'); startStats();
    startCaptureLoop(actualCodec);
    if (onResult) onResult(true);
  };

  ws.onmessage = e => {
    if (typeof e.data === 'string') {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === 'fallback' && msg.codec === 'jpeg') {
          if (encoder) { try { encoder.close(); } catch(_){} encoder = null; }
          onTransportActive('ws-jpeg');
        }
      } catch(_) {}
    }
  };

  ws.onclose = () => { if (isStreaming) { setStatus('error'); cleanUp(); } else if (onResult) onResult(false); };
  ws.onerror = () => { if (onResult) onResult(false); };
}

// ── H.264 WebCodecs encoder ───────────────────────────────────────────────────
async function initH264Encoder(width, height) {
  // Determine candidates based on frame resolution.
  // Level 3.1 (0x1f) is limited to 720p by H.264 specification.
  // 1080p requires Level 4.0 (0x28), Level 4.2 (0x2a), or higher.
  const isHighRes = (width * height > 1280 * 720);
  const candidates = isHighRes
    ? [
        'avc1.42002a', // Baseline Level 4.2 (1080p60)
        'avc1.420028', // Baseline Level 4.0 (1080p30)
        'avc1.4d002a', // Main Level 4.2
        'avc1.64002a', // High Level 4.2
        'avc1.420033', // Baseline Level 5.1 (4K)
        'avc1.640033', // High Level 5.1 (4K)
        'avc1.42001f', // Fallback
      ]
    : [
        'avc1.42001f', // Baseline Level 3.1 (<= 720p)
        'avc1.4d001f', // Main Level 3.1
        'avc1.420028', // Baseline Level 4.0
        'avc1.42002a', // Baseline Level 4.2
        'avc1.64002a', // High Level 4.2
      ];

  const bitrate = (width * height >= 1920 * 1080)
    ? 6_000_000
    : (width * height >= 1280 * 720 ? 4_000_000 : 2_000_000);

  let activeCfg = null;
  for (const c of candidates) {
    for (const accel of ['prefer-hardware', 'no-preference']) {
      for (const latMode of ['realtime', 'quality']) {
        const testCfg = {
          codec: c,
          width,
          height,
          bitrate,
          framerate: TARGET_FPS,
          latencyMode: latMode,
          hardwareAcceleration: accel,
          avc: { format: 'annexb' },
        };
        try {
          const sup = await VideoEncoder.isConfigSupported(testCfg);
          if (sup && sup.supported) {
            activeCfg = testCfg;
            break;
          }
        } catch (_) {}
      }
      if (activeCfg) break;
    }
    if (activeCfg) break;
  }

  if (!activeCfg) {
    throw new Error(`H.264 encoding not supported for ${width}x${height}`);
  }

  console.log(`[H264] Configured: ${activeCfg.codec}, accel=${activeCfg.hardwareAcceleration}, lat=${activeCfg.latencyMode}, ${width}x${height} @ ${bitrate / 1e6}Mbps`);

  encoder = new VideoEncoder({
    output: (chunk, meta) => {
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
      const data = new Uint8Array(chunk.byteLength);
      chunk.copyTo(data);
      let payload = data;
      const hasStartCode = (data.length >= 4 && data[0] === 0 && data[1] === 0 && (data[2] === 1 || (data[2] === 0 && data[3] === 1)));
      if (!hasStartCode && chunk.type === 'key' && meta?.decoderConfig?.description) {
        const desc = new Uint8Array(meta.decoderConfig.description);
        const buf = new Uint8Array(desc.length + data.length);
        buf.set(desc, 0);
        buf.set(data, desc.length);
        payload = buf;
      }
      const pkt = new Uint8Array(9 + payload.byteLength);
      pkt[0] = 0x03; // Tag 0x03: video frame with uint64 capture timestamp (ms)
      new DataView(pkt.buffer).setBigUint64(1, BigInt(Date.now()), false);
      pkt.set(payload, 9);
      ws.send(pkt.buffer);
      bytesSent += pkt.byteLength;
      framesSent++;
    },
    error: e => {
      console.error('[H264] Encoder error:', e);
      if (encoder) {
        try { encoder.close(); } catch (_) {}
        encoder = null;
      }
      if (ws && ws.readyState === WebSocket.OPEN) {
        console.warn('[H264] Falling back to JPEG due to encoder error');
        ws.send(JSON.stringify({ type: 'codec', codec: 'jpeg', width, height }));
        onTransportActive('ws-jpeg');
      }
    },
  });

  encoder.configure(activeCfg);
}

// ── Canvas draw (mirrored) ────────────────────────────────────────────────────
function drawMirrored(ctx, canvas, video) {
  ctx.save();
  ctx.translate(canvas.width, 0);
  ctx.scale(-1, 1);
  ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
  ctx.restore();
}

// ── Capture loop ──────────────────────────────────────────────────────────────
function startCaptureLoop(codec) {
  let lastTime = 0;
  const interval = 1000 / TARGET_FPS;
  let isEncoding = false;
  let fi = 0;

  function capture(ts) {
    captureLoop = requestAnimationFrame(capture);
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    if (!hiddenVideo || hiddenVideo.readyState < 2) return;

    // Always keep mirrored local preview updated at screen refresh rate
    drawMirrored(mirrorCtx, mirrorCanvas, hiddenVideo);

    if (codec === 'h264' && encoder && encoder.state === 'configured') {
      if (ts - lastTime < interval) return;
      lastTime = ts;
      drawMirrored(sendCtx, sendCanvas, hiddenVideo);
      const frame = new VideoFrame(sendCanvas, { timestamp: Math.round(performance.now() * 1000) });
      encoder.encode(frame, { keyFrame: fi % 60 === 0 });
      frame.close();
      fi++;
      return;
    }

    // JPEG mode: rate-limited and strictly in-flight gated.
    // Prevents parallel encodings, canvas race conditions, and out-of-order frame delivery.
    if (ts - lastTime < interval) return;
    if (isEncoding) return;
    if (ws.bufferedAmount > 256 * 1024) return;

    lastTime = ts;
    isEncoding = true;
    drawMirrored(sendCtx, sendCanvas, hiddenVideo);
    const captureTs = Date.now();

    sendCanvas.toBlob(blob => {
      if (!blob || !ws || ws.readyState !== WebSocket.OPEN) {
        isEncoding = false;
        return;
      }
      blob.arrayBuffer().then(buf => {
        try {
          if (ws && ws.readyState === WebSocket.OPEN) {
            const pkt = new Uint8Array(9 + buf.byteLength);
            pkt[0] = 0x03; // Tag 0x03: video frame with uint64 capture timestamp (ms)
            new DataView(pkt.buffer).setBigUint64(1, BigInt(captureTs), false);
            pkt.set(new Uint8Array(buf), 9);
            ws.send(pkt.buffer);
            framesSent++;
            bytesSent += pkt.byteLength;
          }
        } finally {
          isEncoding = false;
        }
      }).catch(() => {
        isEncoding = false;
      });
    }, 'image/jpeg', getQuality());
  }

  captureLoop = requestAnimationFrame(capture);
}

// ── WS-only cleanup (for auto fallback) ──────────────────────────────────────
function cleanUpWS() {
  if (captureLoop) { cancelAnimationFrame(captureLoop); captureLoop = null; }
  if (encoder) { try { encoder.close(); } catch(_){} encoder = null; }
  if (ws) { ws.close(); ws = null; }
  mirrorCanvas = null; mirrorCtx = null; sendCanvas = null; sendCtx = null; hiddenVideo = null;
  localVideo.srcObject = null; localVideo.classList.remove('visible'); videoOverlay.classList.remove('hidden');
}

// ── Full cleanup ──────────────────────────────────────────────────────────────
function cleanUp() {
  stopStats();
  cleanUpWS();
  if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; }
  activeMode = null;
  capabilityRow.style.display = 'none';
  setStreamBtn('idle'); setStatus('idle');
}

// ── Init ──────────────────────────────────────────────────────────────────────
(async () => {
  restoreSettings();
  await enumerateCameras();
  restoreCameraSelection();
  setStatus('idle');
})();

// ── Live reload ───────────────────────────────────────────────────────────────
(function () {
  function showToast(msg) {
    const t = document.createElement('div'); t.className = 'reload-toast'; t.textContent = msg;
    document.body.appendChild(t);
    requestAnimationFrame(() => requestAnimationFrame(() => t.classList.add('visible')));
    setTimeout(() => { t.classList.remove('visible'); t.addEventListener('transitionend', () => t.remove(), { once: true }); }, 2000);
  }
  const es = new EventSource('/livereload');
  es.onmessage = e => { if (e.data === 'reload') { showToast('↻  Reloading…'); setTimeout(() => location.reload(), 300); } };
})();

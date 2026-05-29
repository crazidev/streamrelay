/* StreamRelay — Multi-transport streaming client
 * Transport: Auto (WebRTC → WS H.264 → WS JPEG) | WebRTC | WS H.264 | WS JPEG
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
/** @type {'webrtc'|'ws-h264'|'ws-jpeg'} Active transport actually in use */
let activeMode   = null;

// WebRTC
let pc           = null;
let rtcSessionId = null;

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
let lastRtcStats    = null;

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

const MODE_LABELS = { 'webrtc': 'WebRTC', 'ws-h264': 'H.264', 'ws-jpeg': 'JPEG' };
const MODE_CSS    = { 'webrtc': 'stat-codec--webrtc', 'ws-h264': 'stat-codec--h264', 'ws-jpeg': 'stat-codec--jpeg' };

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
function webRTCSupported()    { return typeof RTCPeerConnection !== 'undefined'; }
function webCodecsSupported() { return typeof VideoEncoder !== 'undefined' && typeof VideoFrame !== 'undefined'; }

// ── Stream button ─────────────────────────────────────────────────────────────
streamBtn.addEventListener('click', () => { if (isStreaming) cleanUp(); else startStreaming(); });

// ── Stats ─────────────────────────────────────────────────────────────────────
function startStats() {
  lastStatsTime = performance.now(); lastStatsFrames = 0; lastStatsBytes = 0;
  statsInterval = setInterval(async () => {
    const now = performance.now(), elapsed = (now - lastStatsTime) / 1000;
    if (elapsed < 0.5) return;
    if (activeMode === 'webrtc' && pc) {
      try {
        const stats = await pc.getStats();
        stats.forEach(r => {
          if (r.type === 'outbound-rtp' && r.kind === 'video') {
            statFps.textContent = (r.framesPerSecond || 0).toFixed(0);
            if (lastRtcStats) {
              let prev = 0; lastRtcStats.forEach(p => { if (p.type === 'outbound-rtp' && p.kind === 'video') prev = p.bytesSent || 0; });
              const kbps = Math.round(((r.bytesSent - prev) * 8) / elapsed / 1000);
              statBitrate.textContent = kbps > 1000 ? (kbps / 1000).toFixed(1) + ' Mbps' : kbps + ' kbps';
            }
          }
        });
        lastRtcStats = stats;
      } catch (_) {}
    } else {
      statFps.textContent = String(Math.round((framesSent - lastStatsFrames) / elapsed));
      const kbps = Math.round(((bytesSent - lastStatsBytes) * 8) / elapsed / 1000);
      statBitrate.textContent = kbps > 1000 ? (kbps / 1000).toFixed(1) + ' Mbps' : kbps + ' kbps';
    }
    lastStatsTime = now; lastStatsFrames = framesSent; lastStatsBytes = bytesSent;
  }, 1000);
}

function stopStats() {
  if (statsInterval) { clearInterval(statsInterval); statsInterval = null; }
  statFps.textContent = '0'; statBitrate.textContent = '0 kbps';
  statResolution.textContent = '—'; statCodec.textContent = '—';
  statCodec.className = 'stat-value stat-codec'; lastRtcStats = null;
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
  try {
    localStream = await navigator.mediaDevices.getUserMedia(getConstraints());
  } catch (err) {
    console.error('[Stream] Camera error:', err);
    setStatus('error'); setStreamBtn('idle'); return;
  }
  const track = localStream.getVideoTracks()[0];
  const s = track.getSettings();
  statResolution.textContent = `${s.width || 1280}×${s.height || 720}`;

  const choice = transportSelect.value;

  if (choice === 'auto') {
    await startAuto();
  } else if (choice === 'webrtc') {
    const ok = await tryWebRTC();
    if (!ok) { cleanUp(); alert('WebRTC failed. Try WS H.264 or WS JPEG.'); }
  } else if (choice === 'ws-h264') {
    await startWebSocket('h264', /*allowFallback=*/false);
  } else {
    await startWebSocket('jpeg', false);
  }
}

/**
 * Auto mode: try WebRTC → WS H.264 → WS JPEG in order.
 * Each failure is silent; the next is tried automatically.
 */
async function startAuto() {
  // 1. WebRTC
  if (webRTCSupported()) {
    console.log('[Auto] Trying WebRTC...');
    const ok = await tryWebRTC();
    if (ok) return;
    console.log('[Auto] WebRTC failed, trying WS H.264...');
  }
  // 2. WS H.264
  if (webCodecsSupported()) {
    console.log('[Auto] Trying WS H.264...');
    const ok = await tryWebSocket('h264');
    if (ok) return;
    console.log('[Auto] WS H.264 failed, falling back to WS JPEG...');
  }
  // 3. WS JPEG (always works)
  console.log('[Auto] Using WS JPEG');
  await startWebSocket('jpeg', false);
}

// ── WebRTC ────────────────────────────────────────────────────────────────────
/** @returns {Promise<boolean>} true if connected successfully */
async function tryWebRTC() {
  try {
    pc = new RTCPeerConnection({ iceServers: [] });
    localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    localVideo.srcObject = localStream;
    localVideo.classList.add('visible');
    videoOverlay.classList.add('hidden');

    const offer = await pc.createOffer({ offerToReceiveAudio: false, offerToReceiveVideo: false });
    await pc.setLocalDescription(offer);

    // Wait for ICE gathering (LAN = fast)
    await new Promise(resolve => {
      if (pc.iceGatheringState === 'complete') { resolve(); return; }
      const check = () => { if (pc.iceGatheringState === 'complete') { pc.removeEventListener('icegatheringstatechange', check); resolve(); } };
      pc.addEventListener('icegatheringstatechange', check);
      setTimeout(resolve, 3000);
    });

    const resp = await fetch('/webrtc', { method: 'POST', headers: { 'Content-Type': 'application/sdp' }, body: pc.localDescription.sdp });
    if (!resp.ok) throw new Error(`Server ${resp.status}`);

    rtcSessionId = resp.headers.get('X-Session-Id');
    await pc.setRemoteDescription({ type: 'answer', sdp: await resp.text() });

    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('ICE timeout')), 10000);
      pc.addEventListener('connectionstatechange', () => {
        const st = pc.connectionState;
        if (st === 'connected') { clearTimeout(t); resolve(); }
        else if (st === 'failed' || st === 'closed') { clearTimeout(t); reject(new Error(st)); }
      });
    });

    onTransportActive('webrtc');
    setStatus('active'); setStreamBtn('streaming'); startStats();
    pc.addEventListener('connectionstatechange', () => {
      if (pc && (pc.connectionState === 'failed' || pc.connectionState === 'disconnected') && isStreaming) { setStatus('error'); cleanUp(); }
    });
    return true;
  } catch (err) {
    console.warn('[WebRTC] Failed:', err.message);
    if (pc) { try { pc.close(); } catch(_){} pc = null; }
    localVideo.srcObject = null; localVideo.classList.remove('visible'); videoOverlay.classList.remove('hidden');
    return false;
  }
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
  sW -= sW % 2; sH -= sH % 2;
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
  const cfg = { codec: 'avc1.42001f', width, height, bitrate: 6_000_000, framerate: TARGET_FPS, latencyMode: 'quality', hardwareAcceleration: 'prefer-hardware', avc: { format: 'annexb' } };
  let sup = await VideoEncoder.isConfigSupported(cfg);
  if (!sup.supported) { cfg.hardwareAcceleration = 'no-preference'; sup = await VideoEncoder.isConfigSupported(cfg); }
  if (!sup.supported) throw new Error('H.264 not supported');
  encoder = new VideoEncoder({
    output: (chunk, meta) => {
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
      const data = new Uint8Array(chunk.byteLength); chunk.copyTo(data);
      if (chunk.type === 'key' && meta?.decoderConfig?.description) {
        const desc = new Uint8Array(meta.decoderConfig.description);
        const buf = new Uint8Array(desc.length + data.length); buf.set(desc, 0); buf.set(data, desc.length);
        ws.send(buf.buffer); bytesSent += buf.byteLength;
      } else { ws.send(data.buffer); bytesSent += data.byteLength; }
      framesSent++;
    },
    error: e => { console.error('[H264]', e); if (encoder) { encoder.close(); encoder = null; } },
  });
  encoder.configure(cfg);
}

// ── Canvas draw (mirrored) ────────────────────────────────────────────────────
function drawMirrored(ctx, canvas, video) {
  ctx.save(); ctx.translate(canvas.width, 0); ctx.scale(-1, 1);
  ctx.drawImage(video, 0, 0, canvas.width, canvas.height); ctx.restore();
}

// ── Capture loop ──────────────────────────────────────────────────────────────
function startCaptureLoop(codec) {
  let lastTime = 0; const interval = 1000 / TARGET_FPS;
  let cA = sendCanvas, ctxA = sendCtx;
  let cB = document.createElement('canvas'); cB.width = sendCanvas.width; cB.height = sendCanvas.height;
  let ctxB = cB.getContext('2d', { willReadFrequently: false });
  let useA = true, fi = 0;

  function capture(ts) {
    captureLoop = requestAnimationFrame(capture);
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    if (!hiddenVideo || hiddenVideo.readyState < 2) return;
    if (ts - lastTime < interval) return; lastTime = ts;
    if (ws.bufferedAmount > 512 * 1024) return;

    drawMirrored(mirrorCtx, mirrorCanvas, hiddenVideo);
    const canv = useA ? cA : cB, ctx = useA ? ctxA : ctxB;
    drawMirrored(ctx, canv, hiddenVideo); useA = !useA;

    if (codec === 'h264' && encoder && encoder.state === 'configured') {
      const frame = new VideoFrame(canv, { timestamp: fi * interval * 1000 });
      encoder.encode(frame, { keyFrame: fi % 60 === 0 }); frame.close(); fi++; return;
    }
    canv.toBlob(blob => {
      if (!blob || !ws || ws.readyState !== WebSocket.OPEN) return;
      blob.arrayBuffer().then(buf => { if (!ws || ws.readyState !== WebSocket.OPEN) return; ws.send(buf); framesSent++; bytesSent += buf.byteLength; });
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
  if (pc) { if (rtcSessionId) fetch(`/webrtc/${rtcSessionId}`, { method: 'DELETE' }).catch(() => {}); try { pc.close(); } catch(_){} pc = null; rtcSessionId = null; }
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

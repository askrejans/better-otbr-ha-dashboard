const svg = document.getElementById('graph');
const tooltip = document.getElementById('tooltip');
let snapshot = null;
let previous = null;
let selected = null;
let timer = null;
let nodes = [];
let links = [];
let flows = [];
let zoom = { x: 0, y: 0, k: 1 };
let lastRender = performance.now();
let lastSnapshotAt = 0;
let trafficHistory = [];
let trafficPaused = false;
let selectedTraffic = null;

const $ = id => document.getElementById(id);
$('refresh').onclick = () => load(true);
$('fit').onclick = () => fit();
$('auto').onchange = () => connectLive();
$('traffic-pause').onclick = () => { trafficPaused = !trafficPaused; $('traffic-pause').textContent = trafficPaused ? 'Resume' : 'Pause'; renderTraffic(); };
$('traffic-clear').onclick = () => { trafficHistory = []; selectedTraffic = null; renderTraffic(); };
$('traffic-search').oninput = () => renderTraffic();
$('traffic-direction').onchange = () => renderTraffic();

async function load(force=false) {
  const res = await fetch(force ? '/api/refresh' : '/api/snapshot', { cache: 'no-store' });
  applySnapshot(await res.json());
}

function connectLive() {
  if (timer) { timer.close?.(); clearTimeout(timer); timer = null; }
  if (!$('auto').checked) return;
  if (window.EventSource) {
    const es = new EventSource('/api/events');
    timer = es;
    es.addEventListener('open', () => setLive(true, 'Live stream connected'));
    es.addEventListener('snapshot', ev => applySnapshot(JSON.parse(ev.data)));
    es.addEventListener('error', () => { setLive(false, 'Live stream reconnecting'); });
    return;
  }
  const poll = async () => {
    try { await load(false); setLive(true, 'Live polling every second'); }
    catch { setLive(false, 'Live polling failed'); }
    timer = setTimeout(poll, 1000);
  };
  poll();
}

function setLive(on, text) {
  $('live-dot').classList.toggle('on', on);
  $('live-text').textContent = text;
}

function applySnapshot(s) {
  previous = snapshot;
  snapshot = s;
  lastSnapshotAt = performance.now();
  const delta = calcDelta(previous, snapshot);
  ingestTraffic(snapshot);
  updatePanels(snapshot, delta);
  mergeGraph(snapshot, delta);
  if (selected) {
    selected = nodes.find(n => n.id === selected.id) || selected;
    showSelected(selected);
  }
}

function calcDelta(a, b) {
  const sec = a && b ? Math.max(1, (new Date(b.generatedAt) - new Date(a.generatedAt)) / 1000) : 0;
  const get = (o, path) => path.reduce((v,k) => v?.[k], o) ?? 0;
  const rx = get(b, ['summary','macRxTotal']) - get(a, ['summary','macRxTotal']);
  const tx = get(b, ['summary','macTxTotal']) - get(a, ['summary','macTxTotal']);
  const iprx = get(b, ['summary','ipRxSuccess']) - get(a, ['summary','ipRxSuccess']);
  const iptx = get(b, ['summary','ipTxSuccess']) - get(a, ['summary','ipTxSuccess']);
  return { sec, rx: Math.max(0, rx || 0), tx: Math.max(0, tx || 0), iprx: Math.max(0, iprx || 0), iptx: Math.max(0, iptx || 0) };
}

function updatePanels(s, d) {
  const sm = s.summary || {};
  $('subtitle').textContent = `${sm.networkName || 'Thread'} · ${new Date(s.generatedAt).toLocaleString()}`;
  $('state').textContent = sm.state || '-';
  $('routers').textContent = sm.routers ?? '-';
  $('children').textContent = sm.children ?? '-';
  $('weak').textContent = sm.weakLinks ?? '-';
  $('macrx').textContent = rateText(sm.macRxTotal, d.rx, d.sec);
  $('mactx').textContent = rateText(sm.macTxTotal, d.tx, d.sec);
  $('iprx').textContent = rateText(sm.ipRxSuccess, d.iprx, d.sec);
  $('iptx').textContent = rateText(sm.ipTxSuccess, d.iptx, d.sec);
  $('graph-summary').textContent = `${sm.routers ?? 0} routers · ${sm.children ?? 0} children · ${sm.weakLinks ?? 0} weak`;
  $('warnings').innerHTML = '';
  const warnings = s.warnings?.length ? s.warnings : [];
  if (!warnings.length) $('warnings').innerHTML = '<li class="muted">None</li>';
  warnings.forEach(w => { const li = document.createElement('li'); li.textContent = w; $('warnings').appendChild(li); });
  $('matter').innerHTML = '';
  (s.matterDevices || []).slice(0, 80).forEach(dv => {
    const div = document.createElement('div'); div.className = 'matter-item';
    div.innerHTML = `<strong>${esc(dv.name)}</strong><span>Node ${esc(dv.nodeId)} · ${esc(dv.manufacturer || '')} ${esc(dv.model || '')}</span>`;
    $('matter').appendChild(div);
  });
  renderTraffic();
  $('raw').textContent = JSON.stringify(s.raw || {}, null, 2);
}
function rateText(total, delta, sec) { return sec ? `${total ?? '-'} (+${delta}, ${(delta/sec).toFixed(1)}/s)` : `${total ?? '-'}`; }

function ingestTraffic(s) {
  if (trafficPaused) return;
  const generatedAt = s.generatedAt || new Date().toISOString();
  const seen = new Set(trafficHistory.map(t => t.key));
  const base = new Date(generatedAt).getTime() || Date.now();
  const incoming = (s.traffic || []).map(ev => {
    const path = ev.pathLabels?.length ? ev.pathLabels.join(' -> ') : fallbackPath(ev);
    const eventAt = Math.round((base - ageMs(ev.age || '')) / 1000);
    const key = `${eventAt}|${ev.direction}|${ev.peer}|${ev.type}|${ev.bytes}|${ev.src || ''}|${ev.dst || ''}|${path}`;
    return { ...ev, pathText: path, key, seenAt: generatedAt, eventAt };
  }).filter(ev => !seen.has(ev.key));
  if (incoming.length) {
  trafficHistory = [...incoming, ...trafficHistory].slice(0, 900);
  }
}

function ageMs(age) {
  const m = String(age).match(/^(\d+):(\d+):(\d+(?:\.\d+)?)$/);
  if (!m) return 0;
  return ((Number(m[1]) * 3600) + (Number(m[2]) * 60) + Number(m[3])) * 1000;
}

function fallbackPath(ev) {
  const name = ev.note || ev.peer || '-';
  return ev.direction === 'tx' ? `OTBR -> ${name}` : `${name} -> OTBR`;
}

function renderTraffic() {
  const q = $('traffic-search').value.trim().toLowerCase();
  const dir = $('traffic-direction').value;
  const list = trafficHistory.filter(ev => {
    if (dir !== 'all' && ev.direction !== dir) return false;
    if (!q) return true;
    return `${ev.pathText} ${ev.type || ''} ${ev.bytes || ''} ${ev.rssi || ''}`.toLowerCase().includes(q);
  });
  $('traffic-count').textContent = `${list.length} matching · ${trafficHistory.length} retained`;
  $('traffic').innerHTML = '';
  if (!list.length) {
    $('traffic').innerHTML = '<div class="muted">No matching traffic events.</div>';
    return;
  }
  list.slice(0, 220).forEach(ev => {
    const div = document.createElement('div');
    div.className = `traffic-item ${ev.direction} ${selectedTraffic === ev.key ? 'selected' : ''}`;
    div.onclick = () => { selectedTraffic = selectedTraffic === ev.key ? null : ev.key; renderTraffic(); };
    const inferred = ev.pathLabels?.length > 2 ? 'inferred path' : 'direct border event';
    const age = ev.age || '-';
    const endpoint = [ev.src ? `src ${ev.src}` : '', ev.dst ? `dst ${ev.dst}` : ''].filter(Boolean).join(' · ');
    div.innerHTML = `<div class="traffic-main"><div class="traffic-head"><span class="traffic-badge">${esc(ev.direction || '-')}</span><span class="traffic-time">${esc(age)}</span><span class="traffic-proto">${esc(ev.type || '-')}</span><span>${ev.bytes || 0} B</span>${ev.rssi ? `<span>RSSI ${ev.rssi}</span>` : ''}<span>${esc(inferred)}</span></div><div class="traffic-path">${esc(ev.pathText)}</div>${endpoint ? `<div class="traffic-endpoints">${esc(endpoint)}</div>` : ''}</div>`;
    $('traffic').appendChild(div);
  });
}

function mergeGraph(s, d) {
  const rect = svg.getBoundingClientRect();
  const w = rect.width || 800, h = rect.height || 600;
  const old = new Map(nodes.map(n => [n.id, n]));
  nodes = (s.nodes || []).map((n, i) => {
    const o = old.get(n.id);
    const angle = (Math.PI * 2 * i) / Math.max(1, (s.nodes || []).length);
    return {
      ...o,
      ...n,
      x: o?.x ?? w/2 + Math.cos(angle) * 150,
      y: o?.y ?? h/2 + Math.sin(angle) * 150,
      vx: o?.vx ?? 0,
      vy: o?.vy ?? 0,
      seenAt: performance.now()
    };
  });
  const map = new Map(nodes.map(n => [n.id, n]));
  const oldLinks = new Map(links.map(l => [linkKey(l), l]));
  const volume = d.rx + d.tx + d.iprx + d.iptx;
  links = (s.links || []).map(l => {
    const o = oldLinks.get(`${l.source}->${l.target}`) || oldLinks.get(`${l.target}->${l.source}`);
    const active = (l.activeBytes || 0) + (l.activePackets || 0) * 30;
    return {
      ...o,
      ...l,
      sourceNode: map.get(l.source),
      targetNode: map.get(l.target),
      volume,
      smoothVolume: o ? o.smoothVolume * 0.55 + Math.max(volume, active) * 0.45 : Math.max(volume, active),
      activeScore: active
    };
  }).filter(l => l.sourceNode && l.targetNode);
  for (let i=0;i<8;i++) tick(w,h,1);
  spawnFlows(d);
  draw();
}

function linkKey(l) { return `${l.source}->${l.target}`; }

function spawnFlows(d) {
  if (!links.length) return;
  const live = d.rx + d.tx + d.iprx + d.iptx;
  const active = links.reduce((sum, l) => sum + (l.activeScore || 0), 0);
  const base = live > 0 || active > 0 ? 1 : 0;
  const countRx = Math.min(28, base + Math.ceil((d.rx + d.iprx + active) / 70));
  const countTx = Math.min(22, base + Math.ceil((d.tx + d.iptx + active) / 90));
  const sorted = [...links].sort((a,b) => ((b.activeScore || 0) + (b.smoothVolume || 0)) - ((a.activeScore || 0) + (a.smoothVolume || 0)));
  for (let i=0;i<countRx;i++) flows.push({ link: sorted[i % sorted.length], dir: 'rx', t: Math.random()*0.22, speed: 0.007 + Math.random()*0.01 });
  for (let i=0;i<countTx;i++) flows.push({ link: sorted[i % sorted.length], dir: 'tx', t: Math.random()*0.22, speed: 0.008 + Math.random()*0.011 });
  flows = flows.slice(-120);
}

function tick(w,h,scale=1) {
  const cx = w/2, cy = h/2;
  for (const n of nodes) { n.vx += (cx - n.x) * 0.0005 * scale; n.vy += (cy - n.y) * 0.0005 * scale; }
  for (let i=0;i<nodes.length;i++) for (let j=i+1;j<nodes.length;j++) {
    const a=nodes[i], b=nodes[j]; let dx=b.x-a.x, dy=b.y-a.y, d2=dx*dx+dy*dy || 1;
    const f = Math.min(2300/d2, 1.4) * scale; const d = Math.sqrt(d2); dx/=d; dy/=d;
    a.vx -= dx*f; a.vy -= dy*f; b.vx += dx*f; b.vy += dy*f;
  }
  for (const l of links) {
    const a=l.sourceNode, b=l.targetNode; let dx=b.x-a.x, dy=b.y-a.y, d=Math.sqrt(dx*dx+dy*dy)||1;
    const target = l.kind === 'mesh' || l.kind === 'relay' ? 115 : b.kind === 'router' ? 190 : 145; const f=(d-target)*0.008*scale; dx/=d; dy/=d;
    a.vx += dx*f; a.vy += dy*f; b.vx -= dx*f; b.vy -= dy*f;
  }
  for (const n of nodes) {
    if (n.kind === 'border-router') { n.x = cx; n.y = cy; n.vx = 0; n.vy = 0; continue; }
    n.vx *= 0.9; n.vy *= 0.9; n.x += n.vx; n.y += n.vy;
    n.x = Math.max(40, Math.min(w-40, n.x)); n.y = Math.max(40, Math.min(h-40, n.y));
  }
}

function draw() {
  svg.innerHTML = '';
  const g = el('g', { transform: `translate(${zoom.x},${zoom.y}) scale(${zoom.k})` }); svg.appendChild(g);
  for (const l of links) {
    const width = 1.2 + Math.min(8, Math.sqrt(l.smoothVolume || l.volume || 0) * 0.32);
    const active = (l.activeScore || 0) > 0;
    g.appendChild(el('line', { class: `link ${l.kind || ''} ${active ? 'active' : ''} ${(l.lqi <= 1 || l.rssi <= -85) ? 'weak' : ''}`, 'stroke-width': width.toFixed(1), x1:l.sourceNode.x, y1:l.sourceNode.y, x2:l.targetNode.x, y2:l.targetNode.y }));
  }
  for (const f of flows) {
    if (!f.link?.sourceNode || !f.link?.targetNode) continue;
    const a = f.dir === 'tx' ? f.link.sourceNode : f.link.targetNode;
    const b = f.dir === 'tx' ? f.link.targetNode : f.link.sourceNode;
    const x = a.x + (b.x - a.x) * f.t, y = a.y + (b.y - a.y) * f.t;
    g.appendChild(el('circle', { class: `flow ${f.dir}`, cx:x, cy:y, r:f.dir === 'rx' ? 3.2 : 2.6, opacity: Math.max(0, 1 - f.t) }));
  }
  for (const n of nodes) {
    const weak = n.lqi <= 1 || n.rssi <= -85;
    const auto = n.linkStatus?.startsWith('auto') || n.linkStatus?.startsWith('inferred') || n.linkStatus?.startsWith('sticky');
    const gn = el('g', { class: `node ${n.kind} ${weak ? 'weak' : ''} ${auto ? 'auto-linked' : ''} ${selected?.id === n.id ? 'selected' : ''}`, transform: `translate(${n.x},${n.y})` });
    const r = n.kind === 'border-router' ? 24 : n.kind === 'router' ? 18 : 14;
    gn.appendChild(el('circle', { r })); gn.appendChild(el('text', { x: r + 7, y: 4 }, short(n.label || n.id, 28)));
    gn.onmouseenter = ev => showTip(ev, n); gn.onmousemove = ev => moveTip(ev); gn.onmouseleave = () => tooltip.hidden = true;
    gn.onpointerdown = ev => { ev.preventDefault(); ev.stopPropagation(); selected = n; showSelected(n); draw(); };
    g.appendChild(gn);
  }
}

function animate(now) {
  const dt = Math.min(50, now - lastRender); lastRender = now;
  const rect = svg.getBoundingClientRect();
  if (nodes.length) tick(rect.width || 800, rect.height || 600, Math.max(0.35, dt / 16));
  for (const f of flows) f.t += f.speed * dt / 16;
  flows = flows.filter(f => f.t <= 1 && f.link && links.includes(f.link));
  for (const l of links) l.smoothVolume = Math.max(0, (l.smoothVolume || 0) * 0.985);
  if (snapshot && performance.now() - lastSnapshotAt > 1600 && $('auto').checked && (typeof EventSource === "undefined" || !(timer instanceof EventSource))) load(false).catch(() => setLive(false, 'Live polling failed'));
  if (nodes.length || flows.length) draw();
  requestAnimationFrame(animate);
}

function showSelected(n) {
  $('selected').className = '';
  const recent = trafficHistory.filter(ev => ev.peer === n.id || ev.path?.includes(n.id)).slice(0, 6);
  const recentHtml = recent.length ? `<br><br><strong>Recent traffic</strong><br>${recent.map(ev => `${esc(ev.pathText || ev.pathLabels?.join(' -> ') || ev.direction)}<br><span class="muted">${esc(ev.age)} · ${ev.bytes || 0} B · ${esc(ev.type || '-')}</span>`).join('<br>')}` : '';
  $('selected').innerHTML = `<strong>${esc(n.label)}</strong><br>Kind: ${esc(n.kind)} / Role: ${esc(n.role || '-') }<br>RLOC16: ${esc(n.rloc16 || '-')}<br>Child ID: ${esc(n.threadChildId || '-')}<br>Ext MAC: ${esc(n.extMac || '-')}<br>SRP host: ${esc(n.srpHost || '-')}<br>Thread IP: ${esc(n.threadIp || '-')}<br>RSSI: ${n.rssi || '-'} / Last: ${n.lastRssi || '-'}<br>Link quality: ${n.lqi || '-'}<br>Age: ${n.age ?? '-'}s<br>Version: ${n.version || '-'}<br>Link: ${esc(n.linkStatus || '-')}<br>${n.note ? `<br><em>${esc(n.note)}</em>` : ''}${n.matterGuess ? `<br><span class="muted">${esc(n.matterGuess)}</span>` : ''}${recentHtml}`;
}
function showTip(ev,n){ tooltip.innerHTML = `<strong>${esc(n.label)}</strong><br>${esc(n.kind)} · ${esc(n.rloc16 || n.id)}<br>RSSI ${n.rssi || '-'} · LQI ${n.lqi || '-'}${n.linkStatus?.startsWith('auto') ? '<br>HA auto-linked' : n.linkStatus?.startsWith('inferred') ? '<br>Inferred name' : n.linkStatus?.startsWith('sticky') ? '<br>Sticky name' : ''}`; tooltip.hidden=false; moveTip(ev); }
function moveTip(ev){ const r=svg.getBoundingClientRect(); tooltip.style.left=(ev.clientX-r.left+12)+'px'; tooltip.style.top=(ev.clientY-r.top+12)+'px'; }
function fit(){ zoom={x:0,y:0,k:1}; draw(); }
function el(name, attrs={}, text=''){ const e=document.createElementNS('http://www.w3.org/2000/svg',name); for(const[k,v]of Object.entries(attrs)) e.setAttribute(k,v); if(text) e.textContent=text; return e; }
function esc(s){ return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function short(s,n){ s=String(s); return s.length>n?s.slice(0,n-3)+'...':s; }
window.addEventListener('resize', () => snapshot && mergeGraph(snapshot, {rx:0,tx:0,iprx:0,iptx:0,sec:0}));
connectLive();
load(false).catch(err => { $('subtitle').textContent = err.message; setLive(false, 'Initial load failed'); });
requestAnimationFrame(animate);

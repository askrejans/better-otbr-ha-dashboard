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
let pollIntervalMs = 10000;
let trafficHistoryLimit = 2000;
let animationFrame = null;
let trafficEnergy = 0;
let flowBudget = 0;
let panDrag = null;
let pendingTrafficEvents = [];
let nodeDrag = null;
let savedNodePositions = loadNodePositions();
const panelDefaults = { status: false, matter: false, traffic: true, health: true };

const $ = id => document.getElementById(id);
$('refresh').onclick = () => load(true);
$('fit').onclick = () => fit();
$('selected-close').onclick = () => clearSelected();
$('auto').onchange = () => connectLive();
$('traffic-pause').onclick = () => { trafficPaused = !trafficPaused; $('traffic-pause').textContent = trafficPaused ? 'Resume' : 'Pause'; if (!trafficPaused) renderTraffic(); };
$('traffic-clear').onclick = () => { trafficHistory = []; selectedTraffic = null; renderTraffic(); };
$('traffic-search').oninput = () => renderTraffic();
$('traffic-direction').onchange = () => renderTraffic();
initPanels();

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
    try { await load(false); setLive(true, `Live polling every ${Math.round(pollIntervalMs / 1000)}s`); }
    catch { setLive(false, 'Live polling failed'); }
    timer = setTimeout(poll, pollIntervalMs);
  };
  poll();
}

function setLive(on, text) {
  $('live-dot').classList.toggle('on', on);
  $('live-text').textContent = text;
}

function applySnapshot(s) {
  if (s?.config?.pollIntervalMs) pollIntervalMs = Math.max(2000, s.config.pollIntervalMs);
  if (s?.config?.trafficHistoryLimit) trafficHistoryLimit = Math.max(50, s.config.trafficHistoryLimit);
  if (snapshot?.generatedAt && snapshot.generatedAt === s?.generatedAt) return;
  previous = snapshot;
  snapshot = s;
  lastSnapshotAt = performance.now();
  const delta = calcDelta(previous, snapshot);
  ingestTraffic(snapshot);
  mergeGraph(snapshot, delta);
  updatePanels(snapshot, delta);
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
  return { sec, rx: rx || 0, tx: tx || 0, iprx: iprx || 0, iptx: iptx || 0 };
}

function updatePanels(s, d) {
  const sm = s.summary || {};
  $('subtitle').textContent = `${sm.networkName || 'Thread'} · ${new Date(s.generatedAt).toLocaleString()}`;
  $('state').textContent = sm.state || '-';
  $('routers').textContent = sm.routers ?? '-';
  $('children').textContent = sm.children ?? '-';
  $('weak').textContent = sm.weakLinks ?? '-';
  $('macrx').textContent = rateText(sm.macRxTotal, d.rx, d.sec, s.fresh?.macCounters, previous?.fresh?.macCounters);
  $('mactx').textContent = rateText(sm.macTxTotal, d.tx, d.sec, s.fresh?.macCounters, previous?.fresh?.macCounters);
  $('iprx').textContent = rateText(sm.ipRxSuccess, d.iprx, d.sec, s.fresh?.ipCounters, previous?.fresh?.ipCounters);
  $('iptx').textContent = rateText(sm.ipTxSuccess, d.iptx, d.sec, s.fresh?.ipCounters, previous?.fresh?.ipCounters);
  $('graph-summary').textContent = `${sm.routers ?? 0} routers · ${sm.children ?? 0} children · ${sm.weakLinks ?? 0} weak`;
  renderHealth(s);
  renderMatterMapping(s);
  if (!trafficPaused) renderTraffic();
  $('raw').textContent = JSON.stringify(s.raw || {}, null, 2);
}

function renderHealth(s) {
  const warnings = s.warnings || [];
  $('warning-count').textContent = warnings.length ? `${warnings.length} issue${warnings.length === 1 ? '' : 's'}` : 'OK';
  $('warnings').innerHTML = '';
  if (!warnings.length) {
    $('warnings').innerHTML = '<div class="health-item ok"><strong>All sources available</strong><span>Latest refresh completed without reported source errors.</span></div>';
    return;
  }
  const frag = document.createDocumentFragment();
  warnings.forEach(w => {
    const div = document.createElement('div');
    div.className = 'health-item warn';
    div.innerHTML = `<strong>${esc(warningSource(w))}</strong><span>${esc(w)}</span>`;
    frag.appendChild(div);
  });
  $('warnings').appendChild(frag);
}

function warningSource(w) {
  const text = String(w || '');
  if (text.includes('OTBR REST')) return 'OTBR REST';
  if (text.includes('batched ot-ctl') || text.includes('neighbor failed') || text.includes('child failed') || text.includes('routerTable failed')) return 'OTBR diagnostics';
  if (text.includes('mac failed') || text.includes('ip failed')) return 'Counters';
  if (text.includes('rx failed') || text.includes('tx failed')) return 'Traffic history';
  if (text.includes('srpHosts failed')) return 'SRP names';
  return 'Source warning';
}

function renderMatterMapping(s) {
  const devices = s.matterDevices || [];
  renderAliasSuggestions(devices);
  const linkedByMatter = new Map();
  (s.nodes || []).forEach(n => {
    if (n.retained || n.linkStatus === 'stale') return;
    const linked = matchMatterNode(n, devices);
    if (linked) {
      const existing = linkedByMatter.get(canonMatterId(linked.nodeId));
      if (!existing || matterMatchScore(n, linked) > matterMatchScore(existing, linked)) {
        linkedByMatter.set(canonMatterId(linked.nodeId), n);
      }
    }
  });
  for (const ev of trafficHistory) {
    const linked = matchMatterTraffic(ev, devices);
    if (!linked) continue;
    const key = canonMatterId(linked.nodeId);
    if (!linkedByMatter.has(key)) {
      linkedByMatter.set(key, {
        id: ev.peer,
        label: ev.note || trafficPeerLabel(ev),
        rloc16: ev.peer?.startsWith('0x') ? ev.peer : '',
        threadIp: trafficPeerIP(ev),
        kind: 'traffic',
        linkStatus: 'traffic-seen'
      });
    }
  }
  $('matter-count').textContent = devices.length ? `${linkedByMatter.size}/${devices.length} linked` : 'none';
  $('matter').innerHTML = '';
  if (!devices.length) {
    $('matter').innerHTML = '<div class="matter-item muted"><strong>No Matter registry devices</strong><span>Mount Home Assistant storage to enable Matter names.</span></div>';
    return;
  }
  const frag = document.createDocumentFragment();
  devices.forEach(dv => {
    const node = linkedByMatter.get(canonMatterId(dv.nodeId));
    const div = document.createElement('button');
    div.type = 'button';
    div.className = `matter-item ${node ? 'linked' : 'unlinked'}`;
    div.disabled = !node;
    div.onclick = () => {
      if (!node) return;
      selected = nodes.find(n => n.id === node.id) || node;
      showSelected(selected);
      draw();
    };
    const status = node ? `${esc(node.label)} · ${esc(node.rloc16 || node.id)} · ${esc(matterMatchSource(node, dv))}` : 'No current Thread match';
    const meta = [dv.manufacturer, dv.model, dv.threadIp ? `IP ${dv.threadIp}` : '', dv.areaId ? `Area ${dv.areaId}` : ''].filter(Boolean).join(' · ');
    div.innerHTML = `<strong>${esc(dv.name)}</strong><span>Node ${esc(dv.nodeId)} · ${status}</span>${meta ? `<span>${esc(meta)}</span>` : ''}`;
    frag.appendChild(div);
  });
  $('matter').appendChild(frag);
}

function renderAliasSuggestions(devices) {
  const list = $('alias-suggestions');
  if (!list) return;
  list.innerHTML = '';
  const seen = new Set();
  devices.forEach(d => {
    const name = String(d.name || '').trim();
    if (!name || seen.has(name)) return;
    seen.add(name);
    const option = document.createElement('option');
    option.value = name;
    list.appendChild(option);
  });
}

function canonMatterId(id) {
  return String(id || '').replace(/^0x/i, '').replace(/^0+/, '').toUpperCase() || '0';
}

function matchMatterNode(n, devices) {
  const mac = String(n.extMac || '').toLowerCase();
  const ip = String(n.threadIp || '').toLowerCase();
  const label = normName(n.label);
  let best = null;
  let bestScore = 0;
  for (const d of devices) {
    const score = matterMatchScore(n, d, mac, ip, label);
    if (score > bestScore) {
      best = d;
      bestScore = score;
    }
  }
  return best;
}
function matterMatchScore(n, d, mac=String(n.extMac || '').toLowerCase(), ip=String(n.threadIp || '').toLowerCase(), label=normName(n.label)) {
  let score = 0;
  if (String(n.id || '') === `matter-${canonMatterId(d.nodeId)}`) score = Math.max(score, 98);
  if (d.extMac && String(d.extMac).toLowerCase() === mac) score = Math.max(score, 100);
  if (d.threadIp && String(d.threadIp).toLowerCase() === ip) score = Math.max(score, 95);
  if (label && label === normName(d.name)) score = Math.max(score, 82);
  if (label && label === normName(`${d.manufacturer || ''} ${d.model || ''}`)) score = Math.max(score, 45);
  if (String(n.linkStatus || '').startsWith('auto') && label === normName(d.name)) score += 8;
  if (String(n.linkStatus || '').startsWith('sticky') && label === normName(d.name)) score += 6;
  return score;
}
function matterMatchSource(n, d) {
  if (String(n.id || '') === `matter-${canonMatterId(d.nodeId)}`) return 'Matter inventory';
  if (d.extMac && String(d.extMac).toLowerCase() === String(n.extMac || '').toLowerCase()) return 'MAC match';
  if (d.threadIp && String(d.threadIp).toLowerCase() === String(n.threadIp || '').toLowerCase()) return 'IP match';
  if (normName(n.label) === normName(d.name)) return 'Name match';
  if (String(n.linkStatus || '') === 'traffic-seen') return 'Seen in traffic';
  return 'Best match';
}
function matchMatterTraffic(ev, devices) {
  const label = normName(ev.note || trafficPeerLabel(ev));
  const ip = trafficPeerIP(ev).toLowerCase();
  return devices.find(d => (label && label === normName(d.name)) || (ip && d.threadIp && ip === String(d.threadIp).toLowerCase()));
}
function normName(v) {
  return String(v || '').trim().toLowerCase().replace(/\s+/g, ' ');
}
function rateText(total, delta, sec, fresh=true, previousFresh=true) {
  if (!fresh) return 'unavailable';
  if (total == null) return '-';
  if (!sec || !previousFresh) return `${total}`;
  if (delta < 0) return `${total} (counter reset)`;
  return `${total} (+${delta}, ${(delta/sec).toFixed(1)}/s over ${Math.round(sec)}s)`;
}

function ingestTraffic(s) {
  const generatedAt = s.generatedAt || new Date().toISOString();
  const seen = new Set(trafficHistory.map(t => t.key));
  const base = new Date(generatedAt).getTime() || Date.now();
  const incoming = (s.traffic || []).map(ev => {
    const path = ev.pathLabels?.length ? ev.pathLabels.join(' -> ') : fallbackPath(ev);
    const eventAtMs = base - ageMs(ev.age || '');
    const eventAt = Math.round(eventAtMs / 1000);
    const key = `${eventAt}|${ev.direction}|${ev.peer}|${ev.type}|${ev.bytes}|${ev.src || ''}|${ev.dst || ''}|${path}`;
    return { ...ev, pathText: path, key, seenAt: generatedAt, eventAt, eventAtMs, localTime: localTimeText(eventAtMs) };
  }).filter(ev => !seen.has(ev.key));
  if (incoming.length) {
    trafficHistory = [...incoming, ...trafficHistory].slice(0, trafficHistoryLimit);
    pendingTrafficEvents.push(...incoming);
  }
}

function localTimeText(ms) {
  const d = new Date(ms);
  if (!Number.isFinite(d.getTime())) return '-';
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
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
  if (snapshot?.config?.trafficEnabled === false) {
    $('traffic-count').textContent = 'Traffic disabled';
    $('traffic').innerHTML = '<div class="muted">Traffic collection is disabled.</div>';
    return;
  }
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
  const frag = document.createDocumentFragment();
  list.forEach(ev => {
    const div = document.createElement('div');
    div.className = `traffic-item ${ev.direction} ${selectedTraffic === ev.key ? 'selected' : ''}`;
    div.onclick = () => { selectedTraffic = selectedTraffic === ev.key ? null : ev.key; renderTraffic(); };
    const inferred = ev.pathLabels?.length > 2 ? 'inferred path' : 'direct border event';
    const time = ev.localTime || localTimeText((ev.eventAt || 0) * 1000);
    const endpoint = [ev.src ? `src ${ev.src}` : '', ev.dst ? `dst ${ev.dst}` : ''].filter(Boolean).join(' · ');
    div.innerHTML = `<div class="traffic-main"><div class="traffic-head"><span class="traffic-badge">${esc(ev.direction || '-')}</span><span class="traffic-time">${esc(time)}</span><span class="traffic-proto">${esc(ev.type || '-')}</span><span>${ev.bytes || 0} B</span>${ev.rssi ? `<span>RSSI ${ev.rssi}</span>` : ''}<span>${esc(inferred)}</span></div><div class="traffic-path">${esc(ev.pathText)}</div>${endpoint ? `<div class="traffic-endpoints">${esc(endpoint)}</div>` : ''}</div>`;
    frag.appendChild(div);
  });
  $('traffic').appendChild(frag);
}

function mergeGraph(s, d) {
  const rect = svg.getBoundingClientRect();
  const w = rect.width || 800, h = rect.height || 600;
  const graphData = augmentGraphWithTraffic(s);
  const old = new Map();
  nodes.forEach(n => {
    for (const key of nodeIdentityAliases(n)) old.set(key, n);
  });
  nodes = (graphData.nodes || []).map((n, i) => {
    const o = nodeIdentityAliases(n).map(key => old.get(key)).find(Boolean);
    const saved = nodeIdentityAliases(n).map(key => savedNodePositions[key]).find(Boolean);
    const angle = (Math.PI * 2 * i) / Math.max(1, (graphData.nodes || []).length);
    return {
      ...o,
      ...n,
      x: o?.x ?? saved?.x ?? w/2 + Math.cos(angle) * 150,
      y: o?.y ?? saved?.y ?? h/2 + Math.sin(angle) * 150,
      vx: o?.vx ?? 0,
      vy: o?.vy ?? 0,
      pinned: o?.pinned || !!saved,
      seenAt: performance.now()
    };
  });
  const map = new Map(nodes.map(n => [n.id, n]));
  const oldLinks = new Map(links.map(l => [linkKey(l), l]));
  const volume = Math.max(0, d.rx) + Math.max(0, d.tx) + Math.max(0, d.iprx) + Math.max(0, d.iptx);
  links = (graphData.links || []).map(l => {
    const o = oldLinks.get(`${l.source}->${l.target}`) || oldLinks.get(`${l.target}->${l.source}`);
    const active = (l.activeBytes || 0) + (l.activePackets || 0) * 30;
    return {
      ...o,
      ...l,
      sourceNode: map.get(l.source),
      targetNode: map.get(l.target),
      volume,
      smoothVolume: o ? o.smoothVolume * 0.72 + Math.max(volume, active) * 0.28 : Math.max(volume, active),
      activeScore: active
    };
  }).filter(l => l.sourceNode && l.targetNode);
  applyPathTraffic(pendingTrafficEvents);
  pendingTrafficEvents = [];
  trafficEnergy = Math.max(trafficEnergy * 0.6, Math.min(260, Math.sqrt(Math.max(0, volume)) * 2.2 + Math.sqrt(Math.max(0, links.reduce((sum, l) => sum + (l.activeScore || 0), 0))) * 1.6));
  for (let i=0;i<8;i++) tick(w,h,1);
  spawnFlows(d);
  draw();
  startAnimation();
}

function linkKey(l) { return `${l.source}->${l.target}`; }
function unorderedLinkKey(a, b) { return a < b ? `${a}--${b}` : `${b}--${a}`; }
function nodeIdentityAliases(n) {
  return [n?.id, n?.extMac, n?.rloc16]
    .filter(v => v && v !== '-')
    .map(v => String(v).toLowerCase());
}

function augmentGraphWithTraffic(s) {
  const baseNodes = [...(s.nodes || [])];
  const baseLinks = [...(s.links || [])];
  const nodeIDs = new Set(baseNodes.map(n => n.id));
  const linkKeys = new Set(baseLinks.map(l => unorderedLinkKey(l.source, l.target)));
  const recentNamed = [...trafficHistory].reverse();
  for (const ev of recentNamed) {
    if (!ev.peer || nodeIDs.has(ev.peer)) continue;
    const label = ev.note || trafficPeerLabel(ev);
    if (!label || label === ev.peer) continue;
    const path = ev.path?.length ? ev.path : [];
    const peerIndex = path.indexOf(ev.peer);
    let neighbor = '';
    if (peerIndex >= 0) {
      neighbor = path[peerIndex - 1] || path[peerIndex + 1] || '';
    }
    if (!neighbor || !nodeIDs.has(neighbor)) neighbor = 'otbr';
    baseNodes.push({
      id: ev.peer,
      label,
      kind: 'traffic',
      role: 'traffic',
      rloc16: ev.peer.startsWith('0x') ? ev.peer : '',
      threadIp: trafficPeerIP(ev),
      note: 'Seen in OTBR traffic history; not present in the latest topology tables',
      alias: true,
      linkStatus: 'traffic-seen'
    });
    nodeIDs.add(ev.peer);
    const lk = unorderedLinkKey(neighbor, ev.peer);
    if (!linkKeys.has(lk)) {
      baseLinks.push({ source: neighbor, target: ev.peer, kind: 'observed', activeBytes: ev.bytes || 0, activePackets: 1 });
      linkKeys.add(lk);
    }
  }
  return { nodes: baseNodes, links: baseLinks };
}

function trafficPeerLabel(ev) {
  const path = ev.path || [];
  const labels = ev.pathLabels || [];
  const idx = path.indexOf(ev.peer);
  return idx >= 0 ? labels[idx] : '';
}

function trafficPeerIP(ev) {
  if (ev.direction === 'rx') return String(ev.src || '').split(':').slice(0, -1).join(':');
  if (ev.direction === 'tx') return String(ev.dst || '').split(':').slice(0, -1).join(':');
  return '';
}

function applyPathTraffic(events) {
  if (!events.length || !links.length) return;
  const directed = new Map();
  const undirected = new Map();
  links.forEach(l => {
    directed.set(`${l.source}->${l.target}`, l);
    directed.set(`${l.target}->${l.source}`, l);
    undirected.set(unorderedLinkKey(l.source, l.target), l);
  });
  for (const ev of events) {
    const path = ev.path?.length ? ev.path : [];
    if (path.length < 2) continue;
    const boost = Math.max(260, (ev.bytes || 0) * 5 + 180);
    for (let i=0;i<path.length-1;i++) {
      const a = path[i], b = path[i+1];
      const link = directed.get(`${a}->${b}`) || undirected.get(unorderedLinkKey(a,b));
      if (!link) continue;
      link.pathActive = (link.pathActive || 0) + boost;
      link.activeScore = (link.activeScore || 0) + boost;
      link.smoothVolume = Math.max(link.smoothVolume || 0, boost);
      addFlow(link, ev.direction || (i === 0 ? 'rx' : 'tx'), Math.random() * 0.12);
      if ((ev.bytes || 0) >= 70) addFlow(link, ev.direction || 'rx', Math.random() * 0.18);
    }
    trafficEnergy = Math.max(trafficEnergy, Math.min(320, boost / 2));
  }
}

function spawnFlows(d) {
  if (!links.length) return;
  const live = Math.max(0, d.rx) + Math.max(0, d.tx) + Math.max(0, d.iprx) + Math.max(0, d.iptx);
  const active = links.reduce((sum, l) => sum + (l.activeScore || 0) + (l.pathActive || 0), 0);
  const base = live > 0 || active > 0 ? 1 : 0;
  const countRx = Math.min(3, base + Math.ceil((Math.max(0, d.rx) + Math.max(0, d.iprx) + active) / 900));
  const countTx = Math.min(3, base + Math.ceil((Math.max(0, d.tx) + Math.max(0, d.iptx) + active) / 1100));
  const sorted = [...links].sort((a,b) => linkActivity(b) - linkActivity(a));
  for (let i=0;i<countRx;i++) addFlow(sorted[i % sorted.length], 'rx', Math.random()*0.22);
  for (let i=0;i<countTx;i++) addFlow(sorted[i % sorted.length], 'tx', Math.random()*0.22);
  flows = flows.slice(-120);
}

function addFlow(link, dir, t=0) {
  if (!link) return;
  flows.push({ link, dir, t, speed: dir === 'rx' ? 0.0048 + Math.random()*0.0048 : 0.0055 + Math.random()*0.0052 });
}

function emitContinuousFlows(dt) {
  if (!links.length || trafficEnergy <= 0.25) return;
  const ranked = links
    .filter(l => l.sourceNode && l.targetNode)
    .sort((a,b) => linkActivity(b) - linkActivity(a));
  if (!ranked.length) return;
  const perSecond = Math.min(18, 0.35 + trafficEnergy / 18);
  flowBudget += perSecond * dt / 1000;
  const maxNew = Math.min(4, Math.floor(flowBudget));
  for (let i=0;i<maxNew;i++) {
    flowBudget -= 1;
    const link = ranked[Math.min(ranked.length - 1, Math.floor(Math.random() * Math.min(ranked.length, 5)))];
    const dir = Math.random() < 0.54 ? 'rx' : 'tx';
    addFlow(link, dir, Math.random() * 0.08);
  }
  flows = flows.slice(-140);
}

function linkActivity(l) {
  return (l.smoothVolume || 0) + (l.activeScore || 0) + (l.pathActive || 0) * 1.4;
}

function tick(w,h,scale=1) {
  const cx = w/2, cy = h/2;
  for (const n of nodes) {
    if (n.pinned) continue;
    n.vx += (cx - n.x) * 0.0005 * scale;
    n.vy += (cy - n.y) * 0.0005 * scale;
  }
  for (let i=0;i<nodes.length;i++) for (let j=i+1;j<nodes.length;j++) {
    const a=nodes[i], b=nodes[j]; let dx=b.x-a.x, dy=b.y-a.y, d2=dx*dx+dy*dy || 1;
    const f = Math.min(2300/d2, 1.4) * scale; const d = Math.sqrt(d2); dx/=d; dy/=d;
    if (!a.pinned) { a.vx -= dx*f; a.vy -= dy*f; }
    if (!b.pinned) { b.vx += dx*f; b.vy += dy*f; }
  }
  for (const l of links) {
    const a=l.sourceNode, b=l.targetNode; let dx=b.x-a.x, dy=b.y-a.y, d=Math.sqrt(dx*dx+dy*dy)||1;
    const target = l.kind === 'mesh' || l.kind === 'relay' ? 115 : b.kind === 'router' ? 190 : 145; const f=(d-target)*0.008*scale; dx/=d; dy/=d;
    if (!a.pinned) { a.vx += dx*f; a.vy += dy*f; }
    if (!b.pinned) { b.vx -= dx*f; b.vy -= dy*f; }
  }
  for (const n of nodes) {
    if (n.kind === 'border-router') { n.x = cx; n.y = cy; n.vx = 0; n.vy = 0; continue; }
    if (n.pinned) { n.vx = 0; n.vy = 0; continue; }
    n.vx *= 0.9; n.vy *= 0.9; n.x += n.vx; n.y += n.vy;
    n.x = Math.max(40, Math.min(w-40, n.x)); n.y = Math.max(40, Math.min(h-40, n.y));
  }
}

function draw() {
  svg.innerHTML = '';
  const g = el('g', { transform: `translate(${zoom.x},${zoom.y}) scale(${zoom.k})` }); svg.appendChild(g);
  for (const l of links) {
    const activity = linkActivity(l);
    const width = 1.2 + Math.min(8, Math.sqrt(activity || l.volume || 0) * 0.32);
    const active = activity > 1;
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
    const stale = n.retained || n.linkStatus === 'stale';
    const weak = !stale && (n.lqi <= 1 || n.rssi <= -85);
    const auto = n.linkStatus?.startsWith('auto') || n.linkStatus?.startsWith('inferred') || n.linkStatus?.startsWith('sticky');
    const gn = el('g', { class: `node ${n.kind} ${weak ? 'weak' : ''} ${stale ? 'stale' : ''} ${auto && !stale ? 'auto-linked' : ''} ${selected?.id === n.id ? 'selected' : ''}`, transform: `translate(${n.x},${n.y})` });
    const r = n.kind === 'border-router' ? 24 : n.kind === 'router' ? 18 : 14;
    gn.appendChild(el('circle', { r })); gn.appendChild(el('text', { x: r + 7, y: 4 }, short(n.label || n.id, 28)));
    gn.onmouseenter = ev => showTip(ev, n); gn.onmousemove = ev => moveTip(ev); gn.onmouseleave = () => { hideTip(); if (!nodeDrag && selected?.id === n.id) clearSelected(); };
    gn.onpointerdown = ev => {
      ev.preventDefault();
      ev.stopPropagation();
      const p = graphPoint(ev);
      nodeDrag = { id: n.id, pointerId: ev.pointerId, dx: n.x - p.x, dy: n.y - p.y };
      gn.setPointerCapture?.(ev.pointerId);
      selected = n;
      showSelected(n);
      draw();
    };
    g.appendChild(gn);
  }
}

function animate(now) {
  const dt = Math.min(50, now - lastRender); lastRender = now;
  const rect = svg.getBoundingClientRect();
  const settling = snapshot && performance.now() - lastSnapshotAt < 2500;
  trafficEnergy = Math.max(0, trafficEnergy * Math.pow(0.5, dt / 22000));
  emitContinuousFlows(dt);
  const activeFlows = flows.length > 0 || trafficEnergy > 0.25;
  if (nodes.length && (settling || activeFlows)) tick(rect.width || 800, rect.height || 600, Math.max(0.35, dt / 16));
  for (const f of flows) f.t += f.speed * dt / 16;
  flows = flows.filter(f => f.t <= 1 && f.link && links.includes(f.link));
  if (settling || activeFlows) {
    for (const l of links) {
      l.smoothVolume = Math.max(0, (l.smoothVolume || 0) * Math.pow(0.5, dt / 26000));
      l.pathActive = Math.max(0, (l.pathActive || 0) * Math.pow(0.5, dt / 42000));
    }
    draw();
  }
  if (settling || flows.length || trafficEnergy > 0.25) {
    animationFrame = requestAnimationFrame(animate);
  } else {
    animationFrame = null;
  }
}

function startAnimation() {
  if (animationFrame) return;
  lastRender = performance.now();
  animationFrame = requestAnimationFrame(animate);
}

function showSelected(n) {
  $('selected-panel').hidden = false;
  $('selected').className = '';
  const recent = trafficHistory.filter(ev => ev.peer === n.id || ev.path?.includes(n.id)).slice(0, 6);
  const recentHtml = recent.length ? `<br><br><strong>Recent traffic</strong><br>${recent.map(ev => `${esc(ev.pathText || ev.pathLabels?.join(' -> ') || ev.direction)}<br><span class="muted">${esc(ev.localTime || localTimeText((ev.eventAt || 0) * 1000))} · ${ev.bytes || 0} B · ${esc(ev.type || '-')}</span>`).join('<br>')}` : '';
  const stale = n.retained || n.linkStatus === 'stale';
  const status = stale ? `Stale, last seen ${n.lastSeenAgo ?? '-'}s ago` : 'Current';
  const aliasKey = preferredAliasKey(n);
  $('selected').innerHTML = `<strong>${esc(n.label)}</strong><br>Status: ${esc(status)}<br>Kind: ${esc(n.kind)} / Role: ${esc(n.role || '-') }<br>RLOC16: ${esc(n.rloc16 || '-')}<br>Child ID: ${esc(n.threadChildId || '-')}<br>Ext MAC: ${esc(n.extMac || '-')}<br>SRP host: ${esc(n.srpHost || '-')}<br>Thread IP: ${esc(n.threadIp || '-')}<br>RSSI: ${stale ? 'last ' : ''}${n.rssi || '-'} / Last: ${n.lastRssi || '-'}<br>Link quality: ${stale ? 'last ' : ''}${n.lqi || '-'}<br>Age: ${n.age ?? '-'}s<br>Version: ${n.version || '-'}<br>Link: ${esc(n.linkStatus || '-')}<br>${n.note ? `<br><em>${esc(n.note)}</em>` : ''}${!stale && n.matterGuess ? `<br><span class="muted">${esc(n.matterGuess)}</span>` : ''}
    <form id="alias-form" class="alias-form">
      <label>Alias</label>
      <div class="alias-row">
        <input id="alias-label" type="text" list="alias-suggestions" value="${esc(n.label || '')}" placeholder="Friendly name">
        <button id="alias-save" type="submit">Save</button>
      </div>
      <span id="alias-status" class="muted">Writes ${esc(aliasKey || 'selected node')} to local aliases.</span>
    </form>${recentHtml}`;
  $('alias-form')?.addEventListener('submit', ev => {
    ev.preventDefault();
    saveAlias(n);
  });
}

function preferredAliasKey(n) {
  if (n.extMac && n.extMac !== '-') return n.extMac.toLowerCase();
  if (n.id && !String(n.id).startsWith('matter-')) return String(n.id).toLowerCase();
  if (n.rloc16) return n.rloc16.toLowerCase();
  return String(n.id || '').toLowerCase();
}

async function saveAlias(n) {
  const input = $('alias-label');
  const status = $('alias-status');
  const button = $('alias-save');
  const label = input?.value?.trim() || '';
  if (!label) {
    if (status) status.textContent = 'Alias label is required.';
    return;
  }
  if (button) button.disabled = true;
  if (status) status.textContent = 'Saving alias...';
  try {
    const res = await fetch('/api/alias', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nodeId: n.id, label })
    });
    if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
    const saved = await res.json();
    if (status) status.textContent = `Saved ${saved.key || 'alias'}.`;
    await load(true);
  } catch (err) {
    if (status) status.textContent = `Save failed: ${String(err.message || err).trim()}`;
  } finally {
    if (button) button.disabled = false;
  }
}
function clearSelected() {
  selected = null;
  $('selected-panel').hidden = true;
  $('selected').className = 'muted';
  $('selected').textContent = 'Click a node.';
  draw();
}
function graphPoint(ev) {
  const rect = svg.getBoundingClientRect();
  return { x: (ev.clientX - rect.left - zoom.x) / zoom.k, y: (ev.clientY - rect.top - zoom.y) / zoom.k };
}
function loadNodePositions() {
  try {
    return JSON.parse(localStorage.getItem('thread-dashboard-node-positions') || '{}') || {};
  } catch {
    return {};
  }
}
function saveNodePositions() {
  const out = {};
  for (const n of nodes) {
    if (n.pinned) out[n.id] = { x: Math.round(n.x), y: Math.round(n.y) };
  }
  savedNodePositions = out;
  try { localStorage.setItem('thread-dashboard-node-positions', JSON.stringify(out)); } catch {}
}
function initPanels() {
  document.querySelectorAll('[data-panel]').forEach(panel => {
    const id = panel.dataset.panel;
    const stored = localStorage.getItem(`thread-dashboard-panel-${id}`);
    const collapsed = stored == null ? !!panelDefaults[id] : stored === 'collapsed';
    setPanelCollapsed(panel, collapsed);
    panel.querySelector('.panel-toggle')?.addEventListener('click', ev => {
      ev.preventDefault();
      const next = !panel.classList.contains('collapsed');
      setPanelCollapsed(panel, next);
      try { localStorage.setItem(`thread-dashboard-panel-${id}`, next ? 'collapsed' : 'open'); } catch {}
    });
  });
}
function setPanelCollapsed(panel, collapsed) {
  panel.classList.toggle('collapsed', collapsed);
}
function showTip(ev,n){
  const stale = n.retained || n.linkStatus === 'stale';
  const match = stale ? `<br>Stale, last seen ${esc(n.lastSeenAgo ?? '-')}s ago` : n.linkStatus?.startsWith('auto') ? '<br>HA auto-linked' : n.linkStatus?.startsWith('inferred') ? '<br>Inferred name' : n.linkStatus?.startsWith('sticky') ? '<br>Sticky name' : '';
  tooltip.innerHTML = `<strong>${esc(n.label)}</strong><br>${esc(n.kind)} · ${esc(n.rloc16 || n.id)}<br>${stale ? 'Last RSSI' : 'RSSI'} ${n.rssi || '-'} · LQI ${n.lqi || '-'}${match}`;
  tooltip.hidden=false; moveTip(ev);
}
function hideTip(){ tooltip.hidden = true; }
function moveTip(ev){ const r=svg.getBoundingClientRect(); tooltip.style.left=(ev.clientX-r.left+12)+'px'; tooltip.style.top=(ev.clientY-r.top+12)+'px'; }
function fit(){
  if (!nodes.length) { zoom={x:0,y:0,k:1}; draw(); return; }
  const rect = svg.getBoundingClientRect();
  const w = rect.width || 800, h = rect.height || 600;
  const pad = 72;
  const xs = nodes.map(n => n.x), ys = nodes.map(n => n.y);
  const minX = Math.min(...xs) - pad, maxX = Math.max(...xs) + pad;
  const minY = Math.min(...ys) - pad, maxY = Math.max(...ys) + pad;
  const bw = Math.max(1, maxX - minX), bh = Math.max(1, maxY - minY);
  const k = Math.min(2.2, Math.max(0.35, Math.min(w / bw, h / bh)));
  zoom = { x: (w - bw * k) / 2 - minX * k, y: (h - bh * k) / 2 - minY * k, k };
  draw();
}
function zoomAt(clientX, clientY, factor) {
  const rect = svg.getBoundingClientRect();
  const px = clientX - rect.left, py = clientY - rect.top;
  const next = Math.min(3.2, Math.max(0.28, zoom.k * factor));
  const wx = (px - zoom.x) / zoom.k, wy = (py - zoom.y) / zoom.k;
  zoom = { x: px - wx * next, y: py - wy * next, k: next };
  draw();
}
function el(name, attrs={}, text=''){ const e=document.createElementNS('http://www.w3.org/2000/svg',name); for(const[k,v]of Object.entries(attrs)) e.setAttribute(k,v); if(text) e.textContent=text; return e; }
function esc(s){ return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function short(s,n){ s=String(s); return s.length>n?s.slice(0,n-3)+'...':s; }
window.addEventListener('resize', () => snapshot && mergeGraph(snapshot, {rx:0,tx:0,iprx:0,iptx:0,sec:0}));
svg.addEventListener('wheel', ev => { ev.preventDefault(); zoomAt(ev.clientX, ev.clientY, Math.exp(-ev.deltaY * 0.0012)); }, { passive: false });
svg.addEventListener('pointermove', ev => { if (!ev.target.closest?.('.node')) hideTip(); });
svg.addEventListener('pointerleave', () => hideTip());
svg.addEventListener('pointerdown', ev => {
  if (!ev.target.closest?.('.node')) hideTip();
  panDrag = { id: ev.pointerId, x: ev.clientX, y: ev.clientY, zx: zoom.x, zy: zoom.y };
  svg.setPointerCapture?.(ev.pointerId);
});
svg.addEventListener('pointermove', ev => {
  if (nodeDrag && nodeDrag.pointerId === ev.pointerId) {
    const n = nodes.find(x => x.id === nodeDrag.id);
    if (!n) return;
    const p = graphPoint(ev);
    n.x = p.x + nodeDrag.dx;
    n.y = p.y + nodeDrag.dy;
    n.vx = 0;
    n.vy = 0;
    n.pinned = true;
    if (selected?.id === n.id) selected = n;
    draw();
    return;
  }
  if (!panDrag || panDrag.id !== ev.pointerId) return;
  zoom.x = panDrag.zx + ev.clientX - panDrag.x;
  zoom.y = panDrag.zy + ev.clientY - panDrag.y;
  draw();
});
svg.addEventListener('pointerup', ev => {
  if (nodeDrag?.pointerId === ev.pointerId) { nodeDrag = null; saveNodePositions(); }
  if (panDrag?.id === ev.pointerId) panDrag = null;
});
svg.addEventListener('pointercancel', ev => {
  if (nodeDrag?.pointerId === ev.pointerId) { nodeDrag = null; saveNodePositions(); }
  if (panDrag?.id === ev.pointerId) panDrag = null;
});
connectLive();
load(false).catch(err => { $('subtitle').textContent = err.message; setLive(false, 'Initial load failed'); });

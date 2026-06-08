package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	ListenAddr    string
	OTBRRestURL   string
	OTBRContainer string
	HAStorage     string
	MatterData    string
	MatterWSURL   string
	AliasFile     string
	PollInterval  time.Duration
	MatterIPTTL   time.Duration
	TopologyTTL   time.Duration
	DockerSocket  string
}

type Snapshot struct {
	GeneratedAt   time.Time         `json:"generatedAt"`
	Network       *NodeInfo         `json:"network,omitempty"`
	Summary       Summary           `json:"summary"`
	Nodes         []GraphNode       `json:"nodes"`
	Links         []GraphLink       `json:"links"`
	Traffic       []TrafficEvent    `json:"traffic"`
	MatterDevices []MatterDevice    `json:"matterDevices"`
	Counters      map[string]any    `json:"counters"`
	Warnings      []string          `json:"warnings"`
	Raw           map[string]string `json:"raw,omitempty"`
}

type Summary struct {
	State       string `json:"state"`
	NetworkName string `json:"networkName"`
	RouterCount int    `json:"routerCount"`
	Children    int    `json:"children"`
	Routers     int    `json:"routers"`
	WeakLinks   int    `json:"weakLinks"`
	MACRxTotal  int64  `json:"macRxTotal"`
	MACTxTotal  int64  `json:"macTxTotal"`
	IPRxSuccess int64  `json:"ipRxSuccess"`
	IPTxSuccess int64  `json:"ipTxSuccess"`
}

type NodeInfo struct {
	BAID        string `json:"baId"`
	State       string `json:"state"`
	RouterCount int    `json:"routerCount"`
	RlocAddress string `json:"rlocAddress"`
	ExtAddress  string `json:"extAddress"`
	NetworkName string `json:"networkName"`
	Rloc16      string `json:"rloc16"`
	RouterID    int    `json:"routerId"`
	ExtPanID    string `json:"extPanId"`
}

type GraphNode struct {
	ID            string `json:"id"`
	Label         string `json:"label"`
	Kind          string `json:"kind"`
	Role          string `json:"role"`
	Rloc16        string `json:"rloc16,omitempty"`
	ExtMAC        string `json:"extMac,omitempty"`
	SRPHost       string `json:"srpHost,omitempty"`
	ThreadIP      string `json:"threadIp,omitempty"`
	ThreadChildID string `json:"threadChildId,omitempty"`
	RSSI          int    `json:"rssi,omitempty"`
	LastRSSI      int    `json:"lastRssi,omitempty"`
	LQI           int    `json:"lqi,omitempty"`
	Age           int    `json:"age,omitempty"`
	Version       int    `json:"version,omitempty"`
	Note          string `json:"note,omitempty"`
	Alias         bool   `json:"alias"`
	MatterGuess   string `json:"matterGuess,omitempty"`
	LinkStatus    string `json:"linkStatus,omitempty"`
}

type GraphLink struct {
	Source        string `json:"source"`
	Target        string `json:"target"`
	Kind          string `json:"kind"`
	RSSI          int    `json:"rssi,omitempty"`
	LQI           int    `json:"lqi,omitempty"`
	Age           int    `json:"age,omitempty"`
	LQIOut        int    `json:"lqiOut,omitempty"`
	PathCost      int    `json:"pathCost,omitempty"`
	ActiveBytes   int    `json:"activeBytes,omitempty"`
	ActivePackets int    `json:"activePackets,omitempty"`
}

type TrafficEvent struct {
	Age        string   `json:"age"`
	Direction  string   `json:"direction"`
	Peer       string   `json:"peer"`
	Type       string   `json:"type"`
	Bytes      int      `json:"bytes"`
	RSSI       int      `json:"rssi,omitempty"`
	Src        string   `json:"src,omitempty"`
	Dst        string   `json:"dst,omitempty"`
	Note       string   `json:"note,omitempty"`
	Path       []string `json:"path,omitempty"`
	PathLabels []string `json:"pathLabels,omitempty"`
}

type RouterHint struct {
	Name   string
	NodeID string
	Score  int
}

type MatterDevice struct {
	NodeID       string `json:"nodeId"`
	ExtMAC       string `json:"extMac,omitempty"`
	ThreadIP     string `json:"threadIp,omitempty"`
	Name         string `json:"name"`
	Model        string `json:"model,omitempty"`
	Manufacturer string `json:"manufacturer,omitempty"`
	AreaID       string `json:"areaId,omitempty"`
	Version      string `json:"version,omitempty"`
}

type StickyName struct {
	Label     string `json:"label"`
	Status    string `json:"status,omitempty"`
	Source    string `json:"source,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

type AliasConfig struct {
	Nodes  map[string]string     `json:"nodes"`
	Notes  map[string]string     `json:"notes"`
	Sticky map[string]StickyName `json:"sticky,omitempty"`
}

type Server struct {
	cfg         Config
	mu          sync.RWMutex
	ss          Snapshot
	matterMu    sync.Mutex
	matterIPs   map[string]string
	matterIPsAt time.Time
	linkMu      sync.Mutex
	stickyLinks map[string]cachedLink
	nodeMu      sync.Mutex
	stickyNodes map[string]cachedNode
}

type cachedLink struct {
	Link   GraphLink
	SeenAt time.Time
}

type cachedNode struct {
	Node   GraphNode
	SeenAt time.Time
}

func main() {
	cfg := loadConfig()
	s := &Server{cfg: cfg}
	if err := s.refresh(context.Background()); err != nil {
		log.Printf("initial refresh failed: %v", err)
	}
	go s.loop()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/snapshot", s.handleSnapshot)
	mux.HandleFunc("/api/refresh", s.handleRefresh)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.Handle("/", noCache(http.FileServer(http.Dir("/static"))))
	log.Printf("better-otbr-ha-dashboard listening on %s", cfg.ListenAddr)
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, mux))
}

func loadConfig() Config {
	interval, err := time.ParseDuration(env("POLL_INTERVAL", "1s"))
	if err != nil {
		interval = time.Second
	}
	return Config{
		ListenAddr:    env("LISTEN_ADDR", ":8888"),
		OTBRRestURL:   strings.TrimRight(env("OTBR_REST_URL", "http://127.0.0.1:8981"), "/"),
		OTBRContainer: env("OTBR_CONTAINER", "otbr"),
		HAStorage:     env("HA_STORAGE", "/ha-storage"),
		MatterData:    env("MATTER_DATA", "/matter-data"),
		MatterWSURL:   env("MATTER_WS_URL", "ws://127.0.0.1:5580/ws"),
		AliasFile:     env("ALIAS_FILE", "/config/aliases.json"),
		PollInterval:  interval,
		MatterIPTTL:   durationEnv("MATTER_IP_TTL", 10*time.Minute),
		TopologyTTL:   durationEnv("TOPOLOGY_LINK_TTL", 5*time.Minute),
		DockerSocket:  env("DOCKER_SOCKET", "/var/run/docker.sock"),
	}
}

func durationEnv(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			return parsed
		}
	}
	return d
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func (s *Server) loop() {
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for range t.C {
		if err := s.refresh(context.Background()); err != nil {
			log.Printf("refresh failed: %v", err)
		}
	}
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	ss := s.ss
	s.mu.RUnlock()
	writeJSON(w, ss)
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if err := s.refresh(r.Context()); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.handleSnapshot(w, r)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	s.writeEvent(w, flusher)
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
			_ = s.refresh(r.Context())
			s.writeEvent(w, flusher)
		}
	}
}

func (s *Server) writeEvent(w io.Writer, flusher http.Flusher) {
	s.mu.RLock()
	ss := s.ss
	s.mu.RUnlock()
	b, _ := json.Marshal(ss)
	fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", b)
	flusher.Flush()
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) refresh(ctx context.Context) error {
	aliases := readAliases(s.cfg.AliasFile)
	matter := readMatterDevices(s.cfg.HAStorage)
	matter = s.enrichMatterIPs(ctx, matter)
	routerHints := readRouterHints(s.cfg.MatterData, matter)
	warnings := []string{}
	raw := map[string]string{}

	node, err := fetchNodeInfo(s.cfg.OTBRRestURL)
	if err != nil {
		warnings = append(warnings, "OTBR REST /node failed: "+err.Error())
	}

	cmds := map[string][]string{
		"state":       {"ot-ctl", "state"},
		"neighbor":    {"ot-ctl", "neighbor", "table"},
		"child":       {"ot-ctl", "child", "table"},
		"mac":         {"ot-ctl", "counters", "mac"},
		"ip":          {"ot-ctl", "counters", "ip"},
		"netdata":     {"ot-ctl", "netdata", "show"},
		"srpHosts":    {"ot-ctl", "srp", "server", "host"},
		"topology":    {"ot-ctl", "history", "netinfo", "list"},
		"routerTable": {"ot-ctl", "router", "table"},
		"rx":          {"ot-ctl", "history", "rx", "list"},
		"tx":          {"ot-ctl", "history", "tx", "list"},
	}
	for name, cmd := range cmds {
		out, err := dockerExec(ctx, s.cfg.DockerSocket, s.cfg.OTBRContainer, cmd)
		if err != nil {
			warnings = append(warnings, name+" failed: "+err.Error())
			continue
		}
		raw[name] = out
	}

	srpHosts := parseSRPHosts(raw["srpHosts"])
	nodes, links, weak := buildGraph(node, raw["neighbor"], raw["child"], raw["topology"], raw["routerTable"], aliases, matter, routerHints, srpHosts)
	nodes = s.stabilizeNodes(nodes)
	links = append(links, readMatterNeighborLinks(s.cfg.MatterData, matter, nodes)...)
	links = s.stabilizeLinks(nodes, links)
	links = ensureRouterAnchors(nodes, links)
	traffic := parseTraffic(raw["rx"], raw["tx"], nodes)
	annotateTrafficPaths(traffic, nodes, links)
	applyTraffic(&links, traffic)
	if changed := rememberDiscoveredNames(s.cfg.AliasFile, &aliases, nodes); changed {
		raw["stickyNames"] = "updated"
	}
	mac := parseCounters(raw["mac"])
	ip := parseCounters(raw["ip"])
	summary := Summary{WeakLinks: weak}
	if node != nil {
		summary.State = node.State
		summary.NetworkName = node.NetworkName
		summary.RouterCount = node.RouterCount
	}
	for _, n := range nodes {
		if n.Kind == "child" {
			summary.Children++
		}
		if n.Kind == "router" {
			summary.Routers++
		}
	}
	summary.MACRxTotal = mac["RxTotal"]
	summary.MACTxTotal = mac["TxTotal"]
	summary.IPRxSuccess = ip["RxSuccess"]
	summary.IPTxSuccess = ip["TxSuccess"]

	ss := Snapshot{GeneratedAt: time.Now(), Network: node, Summary: summary, Nodes: nodes, Links: links, Traffic: traffic, MatterDevices: matter, Counters: map[string]any{"mac": mac, "ip": ip}, Warnings: warnings, Raw: raw}
	s.mu.Lock()
	s.ss = ss
	s.mu.Unlock()
	return nil
}

func fetchNodeInfo(base string) (*NodeInfo, error) {
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(base + "/node")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	var n NodeInfo
	return &n, json.NewDecoder(resp.Body).Decode(&n)
}

func readAliases(path string) AliasConfig {
	a := AliasConfig{Nodes: map[string]string{}, Notes: map[string]string{}, Sticky: map[string]StickyName{}}
	b, err := os.ReadFile(path)
	if err != nil {
		return a
	}
	_ = json.Unmarshal(b, &a)
	if a.Nodes == nil {
		a.Nodes = map[string]string{}
	}
	if a.Notes == nil {
		a.Notes = map[string]string{}
	}
	if a.Sticky == nil {
		a.Sticky = map[string]StickyName{}
	}
	return a
}

func readMatterDevices(storage string) []MatterDevice {
	path := filepath.Join(storage, "core.device_registry")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var doc struct {
		Data struct {
			Devices []map[string]any `json:"devices"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return nil
	}
	var devices []MatterDevice
	re := regexp.MustCompile(`deviceid_[^-]+-([0-9A-Fa-f]{16})-MatterNodeDevice`)
	for _, d := range doc.Data.Devices {
		ids, _ := d["identifiers"].([]any)
		node := ""
		for _, idAny := range ids {
			arr, _ := idAny.([]any)
			if len(arr) < 2 {
				continue
			}
			if fmt.Sprint(arr[0]) != "matter" {
				continue
			}
			m := re.FindStringSubmatch(fmt.Sprint(arr[1]))
			if len(m) == 2 {
				node = strings.TrimLeft(strings.ToUpper(m[1]), "0")
			}
		}
		if node == "" {
			continue
		}
		name := firstString(d["name_by_user"], d["name"], d["model"])
		devices = append(devices, MatterDevice{NodeID: node, Name: name, Model: fmt.Sprint(d["model"]), Manufacturer: fmt.Sprint(d["manufacturer"]), AreaID: fmt.Sprint(d["area_id"]), Version: fmt.Sprint(d["sw_version"])})
	}
	sort.Slice(devices, func(i, j int) bool { return naturalLess(devices[i].NodeID, devices[j].NodeID) })
	return devices
}

func (s *Server) enrichMatterIPs(ctx context.Context, matter []MatterDevice) []MatterDevice {
	if len(matter) == 0 || s.cfg.MatterWSURL == "" {
		return matter
	}
	s.matterMu.Lock()
	defer s.matterMu.Unlock()
	if s.matterIPs != nil && time.Since(s.matterIPsAt) < s.cfg.MatterIPTTL {
		return applyMatterIPs(matter, s.matterIPs)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	ips, err := fetchMatterIPs(lookupCtx, s.cfg.MatterWSURL)
	if err != nil {
		log.Printf("matter ip lookup failed: %v", err)
		if s.matterIPs != nil {
			return applyMatterIPs(matter, s.matterIPs)
		}
		return matter
	}
	log.Printf("matter ip lookup found %d node addresses", len(ips))
	s.matterIPs = ips
	s.matterIPsAt = time.Now()
	return applyMatterIPs(matter, ips)
}

func applyMatterIPs(matter []MatterDevice, ips map[string]string) []MatterDevice {
	out := make([]MatterDevice, len(matter))
	copy(out, matter)
	for i := range out {
		if ip := ips[strings.ToUpper(strings.TrimLeft(out[i].NodeID, "0"))]; ip != "" {
			out[i].ThreadIP = ip
		}
	}
	return out
}

type wsConn struct {
	c   net.Conn
	r   *bufio.Reader
	seq int
}

func fetchMatterIPs(ctx context.Context, wsURL string) (map[string]string, error) {
	addr := strings.TrimPrefix(wsURL, "ws://")
	addr = strings.TrimPrefix(addr, "wss://")
	path := "/"
	if i := strings.Index(addr, "/"); i >= 0 {
		path = addr[i:]
		addr = addr[:i]
	}
	if !strings.Contains(addr, ":") {
		addr += ":80"
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	wc := &wsConn{c: conn, r: bufio.NewReader(conn)}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	defer conn.Close()
	keyBytes := make([]byte, 16)
	_, _ = rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)
	_, _ = fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, addr, key)
	status, err := wc.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.Contains(status, "101") {
		return nil, fmt.Errorf("matter websocket upgrade failed: %s", strings.TrimSpace(status))
	}
	for {
		line, err := wc.r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	if _, err := wc.readText(); err != nil {
		return nil, err
	}
	start, err := wc.command("start_listening", map[string]any{})
	if err != nil {
		return nil, err
	}
	var nodes struct {
		Result []struct {
			NodeID int `json:"node_id"`
		} `json:"result"`
	}
	_ = json.Unmarshal(start, &nodes)
	out := map[string]string{}
	for _, n := range nodes.Result {
		res, err := wc.command("get_node_ip_addresses", map[string]any{"node_id": n.NodeID, "prefer_cache": false, "scoped": false})
		if err != nil {
			continue
		}
		var msg struct {
			Result []string `json:"result"`
		}
		if json.Unmarshal(res, &msg) != nil {
			continue
		}
		for _, ip := range msg.Result {
			if strings.Contains(ip, ":") {
				out[strings.ToUpper(strconv.FormatInt(int64(n.NodeID), 16))] = strings.ToLower(ip)
				break
			}
		}
	}
	return out, nil
}

func (w *wsConn) command(command string, args map[string]any) ([]byte, error) {
	w.seq++
	id := strconv.Itoa(w.seq)
	payload, _ := json.Marshal(map[string]any{"message_id": id, "command": command, "args": args})
	if err := w.writeText(payload); err != nil {
		return nil, err
	}
	for {
		msg, err := w.readText()
		if err != nil {
			return nil, err
		}
		var hdr map[string]any
		if json.Unmarshal(msg, &hdr) != nil || fmt.Sprint(hdr["message_id"]) != id {
			continue
		}
		return msg, nil
	}
}

func (w *wsConn) writeText(payload []byte) error {
	mask := make([]byte, 4)
	_, _ = rand.Read(mask)
	var hdr []byte
	ln := len(payload)
	if ln < 126 {
		hdr = []byte{0x81, byte(0x80 | ln)}
	} else if ln < 65536 {
		hdr = []byte{0x81, 0x80 | 126, byte(ln >> 8), byte(ln)}
	} else {
		return fmt.Errorf("websocket payload too large")
	}
	masked := make([]byte, ln)
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	_, err := w.c.Write(append(append(hdr, mask...), masked...))
	return err
}

func (w *wsConn) readText() ([]byte, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(w.r, h); err != nil {
		return nil, err
	}
	opcode := h[0] & 0x0f
	ln := int(h[1] & 0x7f)
	if ln == 126 {
		b := make([]byte, 2)
		_, _ = io.ReadFull(w.r, b)
		ln = int(binary.BigEndian.Uint16(b))
	} else if ln == 127 {
		b := make([]byte, 8)
		_, _ = io.ReadFull(w.r, b)
		ln = int(binary.BigEndian.Uint64(b))
	}
	masked := h[1]&0x80 != 0
	var mask []byte
	if masked {
		mask = make([]byte, 4)
		_, _ = io.ReadFull(w.r, mask)
	}
	payload := make([]byte, ln)
	if _, err := io.ReadFull(w.r, payload); err != nil {
		return nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	if opcode == 0x8 {
		return nil, io.EOF
	}
	return payload, nil
}

func readRouterHints(dataDir string, matter []MatterDevice) map[string]RouterHint {
	out := map[string]RouterHint{}
	if dataDir == "" {
		return out
	}
	nameByNode := map[string]MatterDevice{}
	for _, d := range matter {
		nameByNode[strings.ToUpper(strings.TrimLeft(d.NodeID, "0"))] = d
	}
	files, _ := filepath.Glob(filepath.Join(dataDir, "*.json"))
	for _, path := range files {
		base := filepath.Base(path)
		if base == "chip.json" || strings.HasSuffix(base, ".backup") {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var doc struct {
			Nodes map[string]struct {
				Attributes map[string]any `json:"attributes"`
			} `json:"nodes"`
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if dec.Decode(&doc) != nil || len(doc.Nodes) == 0 {
			continue
		}
		for nodeID, node := range doc.Nodes {
			dev, ok := nameByNode[canonicalMatterNodeID(nodeID)]
			if !ok || dev.Name == "" {
				continue
			}
			routes, _ := node.Attributes["0/53/8"].([]any)
			for _, routeAny := range routes {
				route, _ := routeAny.(map[string]any)
				if len(route) == 0 {
					continue
				}
				ext := hex64FromAny(route["0"])
				if ext == "" {
					continue
				}
				nextHop := intFromAny(route["3"])
				pathCost := intFromAny(route["4"])
				lqiIn := intFromAny(route["5"])
				lqiOut := intFromAny(route["6"])
				age := intFromAny(route["7"])
				score := lqiIn*40 + lqiOut*40 - pathCost*30 - age
				if nextHop == 0 {
					score += 220
				}
				if role := intFromAny(node.Attributes["0/53/1"]); role == 5 {
					score += 20
				}
				if cur, ok := out[ext]; !ok || score > cur.Score {
					out[ext] = RouterHint{Name: dev.Name, NodeID: canonicalMatterNodeID(nodeID), Score: score}
				}
			}
		}
	}
	return out
}

func readMatterNeighborLinks(dataDir string, matter []MatterDevice, nodes []GraphNode) []GraphLink {
	if dataDir == "" || len(matter) == 0 || len(nodes) == 0 {
		return nil
	}
	nodeByMatter := map[string]string{}
	nodeByExt := map[string]string{}
	nodeByRloc := map[string]string{}
	nodeByLabel := map[string]string{}
	for _, n := range nodes {
		if n.ExtMAC != "" {
			nodeByExt[strings.ToLower(n.ExtMAC)] = n.ID
		}
		if n.Rloc16 != "" {
			nodeByRloc[strings.ToLower(n.Rloc16)] = n.ID
		}
		if n.Label != "" {
			nodeByLabel[n.Label] = n.ID
		}
		if linked := matchMatter(&n, matter); linked != nil {
			nodeByMatter[canonicalMatterNodeID(linked.NodeID)] = n.ID
		}
	}
	for _, d := range matter {
		if nodeByMatter[canonicalMatterNodeID(d.NodeID)] == "" && d.Name != "" {
			nodeByMatter[canonicalMatterNodeID(d.NodeID)] = nodeByLabel[d.Name]
		}
	}
	best := map[string]GraphLink{}
	files, _ := filepath.Glob(filepath.Join(dataDir, "*.json"))
	for _, path := range files {
		if filepath.Base(path) == "chip.json" {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var doc struct {
			Nodes map[string]struct {
				Attributes map[string]any `json:"attributes"`
			} `json:"nodes"`
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if dec.Decode(&doc) != nil {
			continue
		}
		for matterID, node := range doc.Nodes {
			source := nodeByMatter[canonicalMatterNodeID(matterID)]
			if source == "" {
				continue
			}
			neighbors, _ := node.Attributes["0/53/7"].([]any)
			for _, anyRow := range neighbors {
				row, _ := anyRow.(map[string]any)
				if len(row) == 0 {
					continue
				}
				target := nodeByExt[hex64FromAny(row["0"])]
				if target == "" {
					if rloc := rloc16FromAny(row["2"]); rloc != "" {
						target = nodeByRloc[rloc]
					}
				}
				if target == "" || target == source {
					continue
				}
				if target == "otbr" || source == "otbr" {
					continue
				}
				link := GraphLink{Source: source, Target: target, Kind: "observed", LQI: intFromAny(row["5"]), RSSI: intFromAny(row["6"]), Age: intFromAny(row["1"])}
				key := orderedLinkKey(source, target)
				if cur, ok := best[key]; !ok || linkScore(link) > linkScore(cur) {
					best[key] = link
				}
			}
		}
	}
	out := make([]GraphLink, 0, len(best))
	for _, l := range best {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source == out[j].Source {
			return out[i].Target < out[j].Target
		}
		return out[i].Source < out[j].Source
	})
	return out
}

func linkScore(l GraphLink) int {
	return l.LQI*100 + l.RSSI - l.Age/10
}

func canonicalMatterNodeID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if v, err := strconv.ParseUint(id, 10, 64); err == nil {
		return strings.ToUpper(strconv.FormatUint(v, 16))
	}
	id = strings.TrimPrefix(strings.TrimPrefix(id, "0x"), "0X")
	id = strings.TrimLeft(id, "0")
	if id == "" {
		id = "0"
	}
	return strings.ToUpper(id)
}

func hex64FromAny(v any) string {
	switch x := v.(type) {
	case json.Number:
		u, err := strconv.ParseUint(x.String(), 10, 64)
		if err == nil && u > 0 {
			return fmt.Sprintf("%016x", u)
		}
		return ""
	case float64:
		if x <= 0 {
			return ""
		}
		return fmt.Sprintf("%016x", uint64(x))
	case string:
		if x == "" {
			return ""
		}
		u, err := strconv.ParseUint(x, 10, 64)
		if err == nil && u > 0 {
			return fmt.Sprintf("%016x", u)
		}
		return strings.ToLower(strings.TrimPrefix(x, "0x"))
	default:
		return ""
	}
}

func rloc16FromAny(v any) string {
	switch x := v.(type) {
	case json.Number:
		i, err := strconv.ParseInt(x.String(), 10, 64)
		if err == nil {
			return fmt.Sprintf("0x%04x", i)
		}
	case float64:
		return fmt.Sprintf("0x%04x", int(x))
	case int:
		return fmt.Sprintf("0x%04x", x)
	case string:
		if strings.HasPrefix(strings.ToLower(x), "0x") {
			return strings.ToLower(x)
		}
		i, err := strconv.ParseInt(x, 10, 64)
		if err == nil {
			return fmt.Sprintf("0x%04x", i)
		}
	}
	return ""
}

func intFromAny(v any) int {
	switch x := v.(type) {
	case json.Number:
		i, _ := strconv.Atoi(x.String())
		return i
	case float64:
		return int(x)
	case int:
		return x
	case string:
		return atoi(x)
	default:
		return 0
	}
}

func firstString(vals ...any) string {
	for _, v := range vals {
		s := strings.TrimSpace(fmt.Sprint(v))
		if s != "" && s != "<nil>" {
			return s
		}
	}
	return "Unnamed Matter device"
}

func buildGraph(info *NodeInfo, neighborOut, childOut, topologyOut, routerTableOut string, aliases AliasConfig, matter []MatterDevice, routerHints map[string]RouterHint, srpHosts map[string]SRPHost) ([]GraphNode, []GraphLink, int) {
	root := "otbr"
	rootLabel := aliasFor(root, aliases)
	if rootLabel == "" {
		rootLabel = "OTBR"
	}
	nodes := []GraphNode{{ID: root, Label: rootLabel, Kind: "border-router", Role: "leader", Alias: true}}
	if info != nil {
		nodes[0].Rloc16 = info.Rloc16
		nodes[0].ExtMAC = info.ExtAddress
		nodes[0].Role = info.State
	}
	links := []GraphLink{}
	weak := 0
	for _, n := range parseTable(neighborOut) {
		n.Kind = mapRoleKind(n.Role)
		applySRP(&n, srpHosts)
		applyNames(&n, aliases, matter, routerHints)
		if n.LQI <= 1 || n.RSSI <= -85 {
			weak++
		}
		nodes = append(nodes, n)
		links = append(links, GraphLink{Source: root, Target: n.ID, Kind: n.Kind, RSSI: n.RSSI, LQI: n.LQI, Age: n.Age})
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		seen[n.ID] = true
	}
	for _, n := range parseTable(childOut) {
		n.Kind = "child"
		n.Role = "C"
		applySRP(&n, srpHosts)
		applyNames(&n, aliases, matter, routerHints)
		if seen[n.ID] {
			for i := range nodes {
				if nodes[i].ID == n.ID {
					nodes[i].ThreadChildID = n.ThreadChildID
					if nodes[i].LinkStatus != "auto" {
						applyNames(&nodes[i], aliases, matter, routerHints)
					}
				}
			}
			continue
		}
		if n.LQI <= 1 || n.RSSI <= -85 {
			weak++
		}
		nodes = append(nodes, n)
		links = append(links, GraphLink{Source: root, Target: n.ID, Kind: "child", RSSI: n.RSSI, LQI: n.LQI, Age: n.Age})
	}
	addTopologyLinks(&links, nodes, topologyOut)
	addRouterTableLinks(&links, nodes, routerTableOut)
	return nodes, links, weak
}

func addTopologyLinks(links *[]GraphLink, nodes []GraphNode, out string) {
	routerIDToNode := map[string]string{}
	for _, n := range nodes {
		if n.Kind == "border-router" && n.Rloc16 != "" {
			if id := routerIDFromRloc(n.Rloc16); id != "" {
				routerIDToNode[id] = n.ID
			}
		}
		if n.Kind == "router" && n.Rloc16 != "" {
			if id := routerIDFromRloc(n.Rloc16); id != "" {
				routerIDToNode[id] = n.ID
			}
		}
	}
	seen := map[string]bool{}
	for _, l := range *links {
		seen[orderedLinkKey(l.Source, l.Target)] = true
	}
	current := ""
	s := bufio.NewScanner(strings.NewReader(out))
	lineRe := regexp.MustCompile(`id:([0-9]+)\s+rloc16:(0x[0-9a-fA-F]+)`)
	linksRe := regexp.MustCompile(`([0-9]+)-links:\{([^}]*)\}`)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if m := lineRe.FindStringSubmatch(line); len(m) == 3 {
			current = routerIDToNode[m[1]]
			continue
		}
		if current == "" {
			continue
		}
		if m := linksRe.FindStringSubmatch(line); len(m) == 3 {
			lqi := atoi(m[1])
			for _, id := range strings.Fields(m[2]) {
				target := routerIDToNode[strings.TrimLeft(id, "0")]
				if target == "" || target == current {
					continue
				}
				key := orderedLinkKey(current, target)
				if seen[key] {
					continue
				}
				seen[key] = true
				*links = append(*links, GraphLink{Source: current, Target: target, Kind: "mesh", LQI: lqi})
			}
		}
	}
}

func addRouterTableLinks(links *[]GraphLink, nodes []GraphNode, out string) {
	routerIDToNode := map[string]string{}
	for _, n := range nodes {
		if n.Rloc16 == "" || (n.Kind != "router" && n.Kind != "border-router") {
			continue
		}
		if id := routerIDFromRloc(n.Rloc16); id != "" {
			routerIDToNode[id] = n.ID
		}
	}
	seen := map[string]bool{}
	for _, l := range *links {
		seen[orderedLinkKey(l.Source, l.Target)] = true
	}
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if !strings.HasPrefix(line, "|") || strings.Contains(line, "---") || strings.Contains(line, "RLOC16") {
			continue
		}
		parts := splitRow(line)
		if len(parts) < 9 || !strings.HasPrefix(parts[1], "0x") {
			continue
		}
		target := routerIDToNode[strings.TrimSpace(parts[0])]
		nextHopID := strings.TrimSpace(parts[2])
		source := routerIDToNode[nextHopID]
		if target == "" || source == "" || target == source || nextHopID == "63" {
			continue
		}
		key := orderedLinkKey(source, target)
		if seen[key] {
			continue
		}
		seen[key] = true
		*links = append(*links, GraphLink{Source: source, Target: target, Kind: "relay", LQI: atoi(parts[4]), LQIOut: atoi(parts[5]), Age: atoi(parts[6]), PathCost: atoi(parts[3])})
	}
}

func (s *Server) stabilizeNodes(nodes []GraphNode) []GraphNode {
	if s.cfg.TopologyTTL <= 0 {
		return nodes
	}
	now := time.Now()
	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()
	if s.stickyNodes == nil {
		s.stickyNodes = map[string]cachedNode{}
	}
	current := map[string]bool{}
	out := make([]GraphNode, 0, len(nodes)+len(s.stickyNodes))
	for _, n := range nodes {
		out = append(out, n)
		current[n.ID] = true
		if n.Kind != "border-router" {
			s.stickyNodes[n.ID] = cachedNode{Node: n, SeenAt: now}
		}
	}
	for id, cached := range s.stickyNodes {
		if current[id] {
			continue
		}
		if now.Sub(cached.SeenAt) > s.cfg.TopologyTTL {
			delete(s.stickyNodes, id)
			continue
		}
		n := cached.Node
		if n.Note == "" {
			n.Note = "Node retained from recent poll; OTBR did not return it in the latest table"
		}
		out = append(out, n)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return nodeSortKey(out[i]) < nodeSortKey(out[j])
	})
	return out
}

func nodeSortKey(n GraphNode) string {
	switch n.Kind {
	case "border-router":
		return "0:" + n.ID
	case "child":
		return "1:" + n.ID
	case "router":
		return "2:" + n.ID
	default:
		return "3:" + n.ID
	}
}

func (s *Server) stabilizeLinks(nodes []GraphNode, links []GraphLink) []GraphLink {
	if s.cfg.TopologyTTL <= 0 {
		return links
	}
	nodeSeen := map[string]bool{}
	for _, n := range nodes {
		nodeSeen[n.ID] = true
	}
	now := time.Now()
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	if s.stickyLinks == nil {
		s.stickyLinks = map[string]cachedLink{}
	}
	current := map[string]bool{}
	out := make([]GraphLink, 0, len(links)+len(s.stickyLinks))
	for _, l := range links {
		out = append(out, l)
		if !isTopologyLink(l) {
			continue
		}
		key := topologyLinkKey(l)
		current[key] = true
		s.stickyLinks[key] = cachedLink{Link: l, SeenAt: now}
	}
	for key, cached := range s.stickyLinks {
		if current[key] {
			continue
		}
		if now.Sub(cached.SeenAt) > s.cfg.TopologyTTL || !nodeSeen[cached.Link.Source] || !nodeSeen[cached.Link.Target] {
			delete(s.stickyLinks, key)
			continue
		}
		out = append(out, cached.Link)
	}
	return out
}

func isTopologyLink(l GraphLink) bool {
	return l.Kind == "mesh" || l.Kind == "relay" || l.Kind == "observed"
}

func topologyLinkKey(l GraphLink) string {
	if l.Kind == "mesh" {
		return l.Kind + ":" + orderedLinkKey(l.Source, l.Target)
	}
	return l.Kind + ":" + l.Source + "->" + l.Target
}

func ensureRouterAnchors(nodes []GraphNode, links []GraphLink) []GraphLink {
	routers := map[string]bool{}
	for _, n := range nodes {
		if n.Kind == "router" {
			routers[n.ID] = true
		}
	}
	if len(routers) == 0 {
		return links
	}
	direct := directOTBRLinks(links)
	out := make([]GraphLink, 0, len(links)+len(routers))
	out = append(out, links...)
	for id := range routers {
		if direct[id] {
			continue
		}
		out = append(out, GraphLink{Source: "otbr", Target: id, Kind: "anchor"})
	}
	return out
}

func directOTBRLinks(links []GraphLink) map[string]bool {
	out := map[string]bool{}
	for _, l := range links {
		if l.Source == "otbr" {
			out[l.Target] = true
		}
		if l.Target == "otbr" {
			out[l.Source] = true
		}
	}
	return out
}

func routerIDFromRloc(rloc string) string {
	v, err := strconv.ParseInt(strings.TrimPrefix(strings.ToLower(rloc), "0x"), 16, 64)
	if err != nil {
		return ""
	}
	return strconv.FormatInt(v>>10, 10)
}

func orderedLinkKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "--" + b
}

func parseTraffic(rxOut, txOut string, nodes []GraphNode) []TrafficEvent {
	byRloc := map[string]GraphNode{}
	byIP := map[string]GraphNode{}
	for _, n := range nodes {
		if n.Rloc16 != "" {
			byRloc[strings.ToLower(n.Rloc16)] = n
		}
		if n.ThreadIP != "" {
			byIP[strings.ToLower(n.ThreadIP)] = n
		}
	}
	events := append(parseTrafficBlock(rxOut, "rx", byRloc, byIP), parseTrafficBlock(txOut, "tx", byRloc, byIP)...)
	sort.SliceStable(events, func(i, j int) bool { return durationAge(events[i].Age) < durationAge(events[j].Age) })
	if len(events) > 80 {
		events = events[:80]
	}
	return events
}

func parseTrafficBlock(out, dir string, byRloc map[string]GraphNode, byIP map[string]GraphNode) []TrafficEvent {
	var events []TrafficEvent
	var ev *TrafficEvent
	headerRe := regexp.MustCompile(`^(\d\d:\d\d:\d\d\.\d+)`)
	metaRe := regexp.MustCompile(`type:([^\s]+).*len:(\d+).*?(?:from|to):(0x[0-9a-fA-F]+)`)
	rssRe := regexp.MustCompile(`rss:([-0-9]+)`)
	srcRe := regexp.MustCompile(`src:\[([^\]]+)\]:(\d+)`)
	dstRe := regexp.MustCompile(`dst:\[([^\]]+)\]:(\d+)`)
	flush := func() {
		if ev == nil || ev.Peer == "" {
			return
		}
		events = append(events, *ev)
	}
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if m := headerRe.FindStringSubmatch(line); len(m) == 2 {
			flush()
			ev = &TrafficEvent{Age: m[1], Direction: dir}
			continue
		}
		if ev == nil {
			continue
		}
		if m := metaRe.FindStringSubmatch(line); len(m) >= 4 {
			ev.Type = m[1]
			ev.Bytes = atoi(m[2])
			if rm := rssRe.FindStringSubmatch(line); len(rm) == 2 {
				ev.RSSI = atoi(rm[1])
			}
			rloc := strings.ToLower(m[3])
			ev.Peer = rloc
			if n, ok := byRloc[rloc]; ok {
				ev.Peer = n.ID
				ev.Note = n.Label
			}
			continue
		}
		if m := srcRe.FindStringSubmatch(line); len(m) == 3 {
			ev.Src = m[1] + ":" + m[2]
			if n, ok := byIP[strings.ToLower(m[1])]; ok && ev.Note == "" {
				ev.Note = n.Label
			}
			continue
		}
		if m := dstRe.FindStringSubmatch(line); len(m) == 3 {
			ev.Dst = m[1] + ":" + m[2]
			if n, ok := byIP[strings.ToLower(m[1])]; ok && ev.Note == "" {
				ev.Note = n.Label
			}
		}
	}
	flush()
	return events
}

func durationAge(s string) time.Duration {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0
	}
	h := atoi(parts[0])
	m := atoi(parts[1])
	sec, _ := strconv.ParseFloat(parts[2], 64)
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(sec*float64(time.Second))
}

func applyTraffic(links *[]GraphLink, events []TrafficEvent) {
	for _, ev := range events {
		for i := range *links {
			l := &(*links)[i]
			if l.Source == ev.Peer || l.Target == ev.Peer {
				l.ActivePackets++
				l.ActiveBytes += ev.Bytes
				break
			}
		}
	}
}

func annotateTrafficPaths(events []TrafficEvent, nodes []GraphNode, links []GraphLink) {
	labels := map[string]string{}
	for _, n := range nodes {
		labels[n.ID] = n.Label
	}
	relayNext := map[string]string{}
	meshNext := map[string]string{}
	observedBest := map[string]GraphLink{}
	for _, l := range links {
		if l.Kind == "relay" && l.Source != "" && l.Target != "" {
			relayNext[l.Target] = l.Source
		}
		if l.Kind == "mesh" {
			if _, ok := meshNext[l.Target]; !ok {
				meshNext[l.Target] = l.Source
			}
			if _, ok := meshNext[l.Source]; !ok {
				meshNext[l.Source] = l.Target
			}
		}
		if l.Kind == "observed" {
			if cur, ok := observedBest[l.Target]; !ok || linkScore(l) > linkScore(cur) {
				observedBest[l.Target] = l
			}
			reversed := l
			reversed.Source, reversed.Target = l.Target, l.Source
			if cur, ok := observedBest[l.Source]; !ok || linkScore(reversed) > linkScore(cur) {
				observedBest[l.Source] = reversed
			}
		}
	}
	for i := range events {
		peer := events[i].Peer
		if peer == "" {
			continue
		}
		path := []string{"otbr", peer}
		if next := relayNext[peer]; next != "" && next != "otbr" && next != peer {
			path = []string{"otbr", next, peer}
		} else if next := meshNext[peer]; next != "" && next != "otbr" && next != peer {
			path = []string{"otbr", next, peer}
		} else if observed := observedBest[peer]; observed.Source != "" && observed.Source != "otbr" && observed.Source != peer {
			path = []string{"otbr", observed.Source, peer}
		}
		if events[i].Direction == "rx" {
			reverseStrings(path)
		}
		events[i].Path = path
		events[i].PathLabels = make([]string, 0, len(path))
		for _, id := range path {
			label := labels[id]
			if label == "" {
				label = id
			}
			events[i].PathLabels = append(events[i].PathLabels, label)
		}
	}
}

func reverseStrings(v []string) {
	for i, j := 0, len(v)-1; i < j; i, j = i+1, j-1 {
		v[i], v[j] = v[j], v[i]
	}
}

func mapRoleKind(role string) string {
	if role == "R" {
		return "router"
	}
	if role == "C" {
		return "child"
	}
	return "node"
}

func rememberDiscoveredNames(path string, aliases *AliasConfig, nodes []GraphNode) bool {
	if aliases.Sticky == nil {
		aliases.Sticky = map[string]StickyName{}
	}
	changed := false
	now := time.Now().UTC().Format(time.RFC3339)
	for _, n := range nodes {
		if n.Label == "" || n.Label == n.ID || n.Label == n.Rloc16 || n.Label == n.ExtMAC {
			continue
		}
		if !strings.HasPrefix(n.LinkStatus, "auto") && !strings.HasPrefix(n.LinkStatus, "inferred") {
			continue
		}
		sticky := StickyName{Label: n.Label, Status: n.LinkStatus, Source: n.MatterGuess, UpdatedAt: now}
		for _, key := range stickyKeys(n) {
			cur := aliases.Sticky[key]
			if cur.Label != "" && cur.Label != sticky.Label {
				continue
			}
			if cur.Label != "" && strings.HasPrefix(n.LinkStatus, "inferred") {
				continue
			}
			if cur.Label != sticky.Label || cur.Status != sticky.Status || cur.Source != sticky.Source {
				aliases.Sticky[key] = sticky
				changed = true
			}
		}
	}
	if !changed || path == "" {
		return changed
	}
	b, err := json.MarshalIndent(aliases, "", "  ")
	if err != nil {
		return changed
	}
	_ = os.WriteFile(path, append(b, '\n'), 0644)
	return changed
}

func stickyForNode(n *GraphNode, aliases AliasConfig) StickyName {
	for _, key := range stickyKeys(*n) {
		if s := aliases.Sticky[key]; s.Label != "" {
			if strings.Contains(s.Status, "inferred") || strings.Contains(s.Status, "auto-child-id") || strings.Contains(s.Label, "Router near ") {
				continue
			}
			return s
		}
	}
	return StickyName{}
}

func stickyKeys(n GraphNode) []string {
	var keys []string
	if n.ExtMAC != "" {
		keys = append(keys, strings.ToLower(n.ExtMAC))
	}
	if n.Rloc16 != "" {
		keys = append(keys, strings.ToLower(n.Rloc16))
	}
	if n.ID != "" {
		keys = append(keys, strings.ToLower(n.ID))
	}
	return keys
}

func applyNames(n *GraphNode, aliases AliasConfig, matter []MatterDevice, routerHints map[string]RouterHint) {
	if label := aliasFor(n.ID, aliases); label != "" {
		n.Label = label
		n.Alias = true
	}
	if label := aliasFor(n.Rloc16, aliases); label != "" {
		n.Label = label
		n.Alias = true
	}
	if label := aliasFor(n.ExtMAC, aliases); label != "" {
		n.Label = label
		n.Alias = true
	}
	if n.Label == "" {
		n.Label = n.Rloc16
		if n.Label == "" {
			n.Label = n.ID
		}
	}
	if note := aliases.Notes[n.ID]; note != "" {
		n.Note = note
	}
	if note := aliases.Notes[n.Rloc16]; note != "" {
		n.Note = note
	}
	if note := aliases.Notes[n.ExtMAC]; note != "" {
		n.Note = note
	}
	if sticky := stickyForNode(n, aliases); sticky.Label != "" {
		n.Label = sticky.Label
		n.Alias = true
		n.MatterGuess = "Sticky name restored from earlier discovery: " + sticky.Source
		n.LinkStatus = "sticky-" + sticky.Status
		return
	}
	if linked := matchMatter(n, matter); linked != nil {
		n.Label = linked.Name
		n.Alias = true
		n.MatterGuess = "Auto-linked from Home Assistant/Matter"
		n.LinkStatus = "auto"
		return
	}
	// Thread child IDs are transient RLOC child IDs, not Matter node IDs.
	if len(matter) > 0 {
		n.MatterGuess = "No exact HA/Matter MAC/IP match found; add alias if this node should have a friendly name"
		n.LinkStatus = "unlinked"
	}
}

func matchMatter(n *GraphNode, matter []MatterDevice) *MatterDevice {
	mac := strings.ToLower(n.ExtMAC)
	ip := strings.ToLower(n.ThreadIP)
	for i := range matter {
		if matter[i].ExtMAC != "" && strings.ToLower(matter[i].ExtMAC) == mac {
			return &matter[i]
		}
		if matter[i].ThreadIP != "" && strings.ToLower(matter[i].ThreadIP) == ip {
			return &matter[i]
		}
	}
	return nil
}

func matchMatterChildID(n *GraphNode, matter []MatterDevice) *MatterDevice {
	if n.ThreadChildID == "" {
		return nil
	}
	childHex := strings.ToUpper(strings.TrimLeft(n.ThreadChildID, "0"))
	if childHex == "" {
		childHex = "0"
	}
	for i := range matter {
		if strings.ToUpper(matter[i].NodeID) == childHex {
			return &matter[i]
		}
	}
	return nil
}

func aliasFor(k string, a AliasConfig) string {
	if k == "" {
		return ""
	}
	if v := a.Nodes[k]; v != "" {
		return v
	}
	if v := a.Nodes[strings.ToLower(k)]; v != "" {
		return v
	}
	return ""
}

func parseTable(out string) []GraphNode {
	var nodes []GraphNode
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if !strings.HasPrefix(line, "|") || strings.Contains(line, "---") || strings.Contains(line, "Role |") || strings.Contains(line, "ID  |") {
			continue
		}
		parts := splitRow(line)
		if len(parts) >= 10 && (parts[0] == "C" || parts[0] == "R") {
			n := GraphNode{Role: parts[0], Rloc16: parts[1], Age: atoi(parts[2]), RSSI: atoi(parts[3]), LastRSSI: atoi(parts[4]), LQI: atoi(parts[5]), ExtMAC: parts[len(parts)-2], Version: atoi(parts[len(parts)-1])}
			n.ID = idFor(n.Rloc16, n.ExtMAC)
			nodes = append(nodes, n)
		} else if len(parts) >= 12 && strings.HasPrefix(parts[1], "0x") {
			n := GraphNode{Rloc16: parts[1], Age: atoi(parts[3]), LQI: atoi(parts[4]), ExtMAC: parts[len(parts)-1], Version: atoi(parts[8]), ThreadChildID: fmt.Sprintf("%X", atoi(parts[0]))}
			n.ID = idFor(n.Rloc16, n.ExtMAC)
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func splitRow(line string) []string {
	fields := strings.Split(strings.Trim(line, "|"), "|")
	out := []string{}
	for _, f := range fields {
		out = append(out, strings.TrimSpace(f))
	}
	return out
}
func idFor(rloc, mac string) string {
	if rloc != "" {
		return rloc
	}
	return strings.ToLower(mac)
}
func atoi(s string) int { i, _ := strconv.Atoi(strings.TrimSpace(s)); return i }

func parseCounters(out string) map[string]int64 {
	m := map[string]int64{}
	re := regexp.MustCompile(`^\s*([A-Za-z0-9]+):\s*(-?\d+)`)
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		mm := re.FindStringSubmatch(s.Text())
		if len(mm) == 3 {
			v, _ := strconv.ParseInt(mm[2], 10, 64)
			m[mm[1]] = v
		}
	}
	return m
}

func dockerExec(ctx context.Context, sock, container string, cmd []string) (string, error) {
	payload := map[string]any{"AttachStdout": true, "AttachStderr": true, "Tty": false, "Cmd": cmd}
	var created struct {
		ID string `json:"Id"`
	}
	if err := dockerJSON(ctx, sock, "POST", "/containers/"+container+"/exec", payload, &created); err != nil {
		return "", err
	}
	if created.ID == "" {
		return "", errors.New("empty exec id")
	}
	body, err := dockerRaw(ctx, sock, "POST", "/exec/"+created.ID+"/start", map[string]any{"Detach": false, "Tty": false})
	if err != nil {
		return "", err
	}
	return decodeDockerStream(body), nil
}

func dockerJSON(ctx context.Context, sock, method, path string, in any, out any) error {
	body, err := dockerRaw(ctx, sock, method, path, in)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

func dockerRaw(ctx context.Context, sock, method, path string, in any) ([]byte, error) {
	var r io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, "http://docker"+path, r)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}, Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("docker api %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return b, nil
}

func decodeDockerStream(b []byte) string {
	var out bytes.Buffer
	for len(b) >= 8 {
		sz := int(binary.BigEndian.Uint32(b[4:8]))
		if sz < 0 || len(b) < 8+sz {
			break
		}
		out.Write(b[8 : 8+sz])
		b = b[8+sz:]
	}
	if out.Len() == 0 {
		return string(b)
	}
	return out.String()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func naturalLess(a, b string) bool {
	ai, _ := strconv.ParseInt(a, 16, 64)
	bi, _ := strconv.ParseInt(b, 16, 64)
	if ai != bi {
		return ai < bi
	}
	return a < b
}

type SRPHost struct {
	HostName string
	Address  string
}

func parseSRPHosts(out string) map[string]SRPHost {
	hosts := map[string]SRPHost{}
	var current string
	s := bufio.NewScanner(strings.NewReader(out))
	hostRe := regexp.MustCompile(`^([0-9A-Fa-f]{16})\.default\.service\.arpa\.`)
	addrRe := regexp.MustCompile(`addresses:\s*\[([^\]]+)\]`)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if m := hostRe.FindStringSubmatch(line); len(m) == 2 {
			current = strings.ToLower(m[1])
			hosts[current] = SRPHost{HostName: m[1] + ".default.service.arpa."}
			continue
		}
		if current != "" {
			if m := addrRe.FindStringSubmatch(line); len(m) == 2 {
				h := hosts[current]
				h.Address = strings.TrimSpace(m[1])
				hosts[current] = h
			}
		}
	}
	return hosts
}

func applySRP(n *GraphNode, srpHosts map[string]SRPHost) {
	if n.ExtMAC == "" {
		return
	}
	if h, ok := srpHosts[strings.ToLower(n.ExtMAC)]; ok {
		n.SRPHost = h.HostName
		n.ThreadIP = h.Address
	}
}

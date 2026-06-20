package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadConfigDefaultsAreLightweight(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("INCLUDE_RAW", "")
	t.Setenv("INCLUDE_COUNTERS", "")
	t.Setenv("ENABLE_TRAFFIC", "")
	t.Setenv("METADATA_CACHE_TTL", "")
	t.Setenv("NODE_CACHE_TTL", "")
	t.Setenv("TRAFFIC_HISTORY_LIMIT", "")
	t.Setenv("SLOW_POLL_INTERVAL", "")
	t.Setenv("IDLE_POLL_INTERVAL", "")
	t.Setenv("TOPOLOGY_NODE_TTL", "")

	cfg := loadConfig()
	if cfg.PollInterval != 10*time.Second {
		t.Fatalf("PollInterval = %v, want 10s", cfg.PollInterval)
	}
	if cfg.IncludeRaw {
		t.Fatal("IncludeRaw = true, want false")
	}
	if cfg.MetadataCacheTTL != 30*time.Second {
		t.Fatalf("MetadataCacheTTL = %v, want 30s", cfg.MetadataCacheTTL)
	}
	if cfg.NodeCacheTTL != 30*time.Second {
		t.Fatalf("NodeCacheTTL = %v, want 30s", cfg.NodeCacheTTL)
	}
	if cfg.SlowPollInterval != time.Minute {
		t.Fatalf("SlowPollInterval = %v, want 1m", cfg.SlowPollInterval)
	}
	if cfg.TopologyNodeTTL != 90*time.Second {
		t.Fatalf("TopologyNodeTTL = %v, want 90s", cfg.TopologyNodeTTL)
	}
	if cfg.IdlePollInterval != time.Minute {
		t.Fatalf("IdlePollInterval = %v, want 1m", cfg.IdlePollInterval)
	}
	if cfg.IncludeCounters {
		t.Fatal("IncludeCounters = true, want false")
	}
	if !cfg.EnableTraffic {
		t.Fatal("EnableTraffic = false, want true")
	}
	if cfg.TrafficHistoryLimit != 2000 {
		t.Fatalf("TrafficHistoryLimit = %d, want 2000", cfg.TrafficHistoryLimit)
	}
}

func TestLoadConfigClampsPollInterval(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "500ms")

	cfg := loadConfig()
	if cfg.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %v, want 2s clamp", cfg.PollInterval)
	}
}

func TestLoadConfigParsesDebugAndLimits(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "15s")
	t.Setenv("INCLUDE_RAW", "true")
	t.Setenv("INCLUDE_COUNTERS", "true")
	t.Setenv("ENABLE_TRAFFIC", "false")
	t.Setenv("TRAFFIC_HISTORY_LIMIT", "123")

	cfg := loadConfig()
	if cfg.PollInterval != 15*time.Second {
		t.Fatalf("PollInterval = %v, want 15s", cfg.PollInterval)
	}
	if !cfg.IncludeRaw {
		t.Fatal("IncludeRaw = false, want true")
	}
	if !cfg.IncludeCounters {
		t.Fatal("IncludeCounters = false, want true")
	}
	if cfg.EnableTraffic {
		t.Fatal("EnableTraffic = true, want false")
	}
	if cfg.TrafficHistoryLimit != 123 {
		t.Fatalf("TrafficHistoryLimit = %d, want 123", cfg.TrafficHistoryLimit)
	}
}

func TestActiveViewerIntervals(t *testing.T) {
	s := NewServer(Config{PollInterval: 10 * time.Second, IdlePollInterval: time.Minute})
	if got := s.nextRefreshInterval(); got != time.Minute {
		t.Fatalf("idle interval = %v, want 1m", got)
	}
	ch := s.subscribe()
	if got := s.nextRefreshInterval(); got != 10*time.Second {
		t.Fatalf("active interval = %v, want 10s", got)
	}
	s.unsubscribe(ch)
	if got := s.nextRefreshInterval(); got != time.Minute {
		t.Fatalf("idle interval after unsubscribe = %v, want 1m", got)
	}
}

func TestRefreshSerializesConcurrentCalls(t *testing.T) {
	s := NewServer(Config{})
	var active int32
	var maxActive int32
	var calls int32
	s.refreshFunc = func(context.Context) error {
		now := atomic.AddInt32(&active, 1)
		for {
			prev := atomic.LoadInt32(&maxActive)
			if now <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, now) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		atomic.AddInt32(&calls, 1)
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.refresh(context.Background()); err != nil {
				t.Errorf("refresh failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if calls != 8 {
		t.Fatalf("calls = %d, want 8", calls)
	}
	if maxActive != 1 {
		t.Fatalf("max concurrent refreshes = %d, want 1", maxActive)
	}
}

func TestEventSubscribersReceivePublishedSnapshots(t *testing.T) {
	s := NewServer(Config{})
	a := s.subscribe()
	b := s.subscribe()
	defer s.unsubscribe(a)
	defer s.unsubscribe(b)

	ss := Snapshot{GeneratedAt: time.Now()}
	s.publish(ss)

	for name, ch := range map[string]chan Snapshot{"a": a, "b": b} {
		select {
		case got := <-ch:
			if !got.GeneratedAt.Equal(ss.GeneratedAt) {
				t.Fatalf("%s got GeneratedAt %v, want %v", name, got.GeneratedAt, ss.GeneratedAt)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not receive snapshot", name)
		}
	}
}

func TestWriteEventIncludesSnapshotConfig(t *testing.T) {
	s := NewServer(Config{})
	s.ss = Snapshot{GeneratedAt: time.Unix(1, 0).UTC(), Config: SnapshotConfig{PollIntervalMS: 10000, TrafficHistoryLimit: 2000, TrafficEnabled: true}}
	rr := httptest.NewRecorder()

	s.writeEvent(rr, flushRecorder{})

	body := rr.Body.String()
	if !strings.Contains(body, "event: snapshot") {
		t.Fatalf("missing snapshot event: %s", body)
	}
	if !strings.Contains(body, `"pollIntervalMs":10000`) {
		t.Fatalf("missing poll interval config: %s", body)
	}
}

func TestSnapshotVersionIgnoresGeneratedAt(t *testing.T) {
	a := Snapshot{GeneratedAt: time.Unix(1, 0), Summary: Summary{State: "leader"}}
	b := Snapshot{GeneratedAt: time.Unix(2, 0), Summary: Summary{State: "leader"}}
	if snapshotVersion(a) != snapshotVersion(b) {
		t.Fatal("snapshot version should ignore GeneratedAt")
	}
	b.Summary.State = "router"
	if snapshotVersion(a) == snapshotVersion(b) {
		t.Fatal("snapshot version should change when payload changes")
	}
}

func TestIDForPrefersStableExtMAC(t *testing.T) {
	if got := idFor("0x5001", "AA11BB22CC33DD44"); got != "aa11bb22cc33dd44" {
		t.Fatalf("idFor with MAC = %q, want stable lowercase MAC", got)
	}
	if got := idFor(" 0x5001 ", "-"); got != "0x5001" {
		t.Fatalf("idFor placeholder MAC = %q, want RLOC fallback", got)
	}
}

type flushRecorder struct{}

func (flushRecorder) Flush() {}

func TestAliasCacheReusesWithinTTLAndInvalidatesAfterExpiry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aliases.json")
	if err := os.WriteFile(path, []byte(`{"nodes":{"a":"one"},"notes":{},"sticky":{}}`), 0644); err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{MetadataCacheTTL: time.Hour})

	first := s.readAliasesCached(path)
	if first.Nodes["a"] != "one" {
		t.Fatalf("first alias = %q, want one", first.Nodes["a"])
	}
	if err := os.WriteFile(path, []byte(`{"nodes":{"a":"two"},"notes":{},"sticky":{}}`), 0644); err != nil {
		t.Fatal(err)
	}
	second := s.readAliasesCached(path)
	if second.Nodes["a"] != "one" {
		t.Fatalf("cached alias = %q, want one", second.Nodes["a"])
	}

	s.metaMu.Lock()
	s.metaCache.aliases.checked = time.Now().Add(-2 * time.Hour)
	s.metaMu.Unlock()
	third := s.readAliasesCached(path)
	if third.Nodes["a"] != "two" {
		t.Fatalf("expired alias = %q, want two", third.Nodes["a"])
	}
}

func TestMatterFileCacheReusesWithinTTLAndInvalidatesAfterExpiry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.json")
	if err := os.WriteFile(path, []byte(`{"nodes":{}}`), 0644); err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{MetadataCacheTTL: time.Hour})

	first := s.readMatterFilesCached(dir)
	if len(first) != 1 || !bytes.Contains(first[0].data, []byte(`nodes`)) {
		t.Fatalf("unexpected first files: %#v", first)
	}
	if err := os.WriteFile(path, []byte(`{"changed":true}`), 0644); err != nil {
		t.Fatal(err)
	}
	second := s.readMatterFilesCached(dir)
	if !bytes.Equal(second[0].data, first[0].data) {
		t.Fatal("cache should reuse matter file data inside TTL")
	}

	s.metaMu.Lock()
	s.metaCache.files.checked = time.Now().Add(-2 * time.Hour)
	s.metaMu.Unlock()
	third := s.readMatterFilesCached(dir)
	if !bytes.Contains(third[0].data, []byte(`changed`)) {
		t.Fatalf("expired matter file did not reload: %s", third[0].data)
	}
}

func TestParsers(t *testing.T) {
	counters := parseCounters("RxTotal: 42\nTxTotal: 7\nIgnored: nope\n")
	if counters["RxTotal"] != 42 || counters["TxTotal"] != 7 {
		t.Fatalf("unexpected counters: %#v", counters)
	}

	hosts := parseSRPHosts("821988813fe1b531.default.service.arpa.\n    addresses: [fd00::1234]\n")
	if hosts["821988813fe1b531"].Address != "fd00::1234" {
		t.Fatalf("unexpected srp hosts: %#v", hosts)
	}

	stream := append([]byte{1, 0, 0, 0, 0, 0, 0, 5}, []byte("hello")...)
	if got := decodeDockerStream(stream); got != "hello" {
		t.Fatalf("decodeDockerStream = %q, want hello", got)
	}
}

func TestParseBatchOutput(t *testing.T) {
	out := strings.Join([]string{
		"noise before marker",
		batchBegin("neighbor"),
		"| Role | RLOC16 |",
		"| R    | 0x1234 |",
		batchEnd("neighbor"),
		batchBegin("mac"),
		"RxTotal: 42",
		"TxTotal: 7",
		batchEnd("mac"),
	}, "\n")

	got := parseBatchOutput(out)
	if !strings.Contains(got["neighbor"], "0x1234") {
		t.Fatalf("neighbor output missing table: %#v", got)
	}
	if got["mac"] != "RxTotal: 42\nTxTotal: 7" {
		t.Fatalf("mac output = %q", got["mac"])
	}
	if _, ok := got["noise"]; ok {
		t.Fatalf("unexpected noise key: %#v", got)
	}
}

func TestTrafficPathAnnotation(t *testing.T) {
	nodes := []GraphNode{
		{ID: "otbr", Label: "OTBR"},
		{ID: "router", Label: "Router"},
		{ID: "child", Label: "Child"},
	}
	links := []GraphLink{{Source: "router", Target: "child", Kind: "relay"}}
	events := []TrafficEvent{{Direction: "rx", Peer: "child"}}

	annotateTrafficPaths(events, nodes, links)

	got := strings.Join(events[0].PathLabels, " -> ")
	if got != "Child -> Router -> OTBR" {
		t.Fatalf("path = %q, want Child -> Router -> OTBR", got)
	}
}

func TestBuildGraphDeduplicatesNodeWhenRoleChanges(t *testing.T) {
	neighbor := "| R | 0x5000 | 1 | -50 | -51 | 3 | - | - | AA11BB22CC33DD44 | 4 |"
	child := "| 1 | 0x8401 | 0 | 12 | 3 | - | - | - | 4 | - | - | AA11BB22CC33DD44 |"

	nodes, links, _ := buildGraph(nil, neighbor, child, "", "", AliasConfig{}, nil, nil, nil)

	var matches []GraphNode
	for _, n := range nodes {
		if n.ExtMAC == "AA11BB22CC33DD44" {
			matches = append(matches, n)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("nodes with same ext MAC = %d, want 1: %#v", len(matches), matches)
	}
	if matches[0].ID != "aa11bb22cc33dd44" {
		t.Fatalf("node ID = %q, want stable ext MAC", matches[0].ID)
	}
	if matches[0].ThreadChildID != "1" {
		t.Fatalf("ThreadChildID = %q, want merged child ID", matches[0].ThreadChildID)
	}
	for _, l := range links {
		if l.Target == "0x8401" || l.Target == "0x5000" {
			t.Fatalf("link still targets transient RLOC: %#v", l)
		}
	}
}

func TestStabilizeNodesMarksMissingNodeStale(t *testing.T) {
	s := NewServer(Config{TopologyNodeTTL: time.Minute, TopologyTTL: 5 * time.Minute})
	current := []GraphNode{{ID: "aa", Label: "Lamp", Kind: "router", LinkStatus: "sticky-auto", MatterGuess: "Sticky name restored from earlier discovery: Auto-linked from Home Assistant/Matter"}}

	s.stabilizeNodes(current)
	s.stickyNodeMu.Lock()
	cached := s.stickyNodes["aa"]
	cached.SeenAt = time.Now().Add(-20 * time.Second)
	s.stickyNodes["aa"] = cached
	s.stickyNodeMu.Unlock()

	got := s.stabilizeNodes(nil)
	if len(got) != 1 {
		t.Fatalf("retained nodes = %d, want 1", len(got))
	}
	if !got[0].Retained {
		t.Fatal("retained node Retained = false, want true")
	}
	if got[0].LinkStatus != "stale" {
		t.Fatalf("LinkStatus = %q, want stale", got[0].LinkStatus)
	}
	if got[0].MatterGuess != "" {
		t.Fatalf("MatterGuess = %q, want empty for stale node", got[0].MatterGuess)
	}
}

func TestStabilizeNodesExpiresByNodeTTL(t *testing.T) {
	s := NewServer(Config{TopologyNodeTTL: 30 * time.Second, TopologyTTL: 5 * time.Minute})
	s.stabilizeNodes([]GraphNode{{ID: "aa", Label: "Lamp", Kind: "router"}})
	s.stickyNodeMu.Lock()
	cached := s.stickyNodes["aa"]
	cached.SeenAt = time.Now().Add(-31 * time.Second)
	s.stickyNodes["aa"] = cached
	s.stickyNodeMu.Unlock()

	got := s.stabilizeNodes(nil)
	if len(got) != 0 {
		t.Fatalf("retained nodes = %d, want expired", len(got))
	}
}

func TestParseTrafficDoesNotCapEvents(t *testing.T) {
	var rx strings.Builder
	for i := 0; i < 120; i++ {
		fmt.Fprintf(&rx, "00:00:%02d.000\n", i%60)
		rx.WriteString("type:UDP len:64 from:0x1234 rss:-60\n")
	}
	nodes := []GraphNode{{ID: "router", Rloc16: "0x1234", Label: "Router"}}

	events := parseTraffic(rx.String(), "", nodes, nil)
	if len(events) != 120 {
		t.Fatalf("events = %d, want 120", len(events))
	}
}

func TestLimitTrafficEventsKeepsNewestSortedEvents(t *testing.T) {
	events := []TrafficEvent{
		{Age: "00:00:01.000", Peer: "a"},
		{Age: "00:00:02.000", Peer: "b"},
		{Age: "00:00:03.000", Peer: "c"},
	}

	got := limitTrafficEvents(events, 2)
	if len(got) != 2 {
		t.Fatalf("events = %d, want 2", len(got))
	}
	if got[0].Peer != "a" || got[1].Peer != "b" {
		t.Fatalf("kept events = %#v, want newest two by sorted age", got)
	}
}

func TestParseTrafficMapsRLOCToStableNodeID(t *testing.T) {
	rx := strings.Join([]string{
		"00:00:01.000",
		"type:UDP len:64 from:0x1234 rss:-60",
	}, "\n")
	nodes := []GraphNode{{ID: "aa11bb22cc33dd44", Rloc16: "0x1234", ExtMAC: "AA11BB22CC33DD44", Label: "Dimmer"}}

	events := parseTraffic(rx, "", nodes, nil)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Peer != "aa11bb22cc33dd44" {
		t.Fatalf("peer = %q, want stable node ID", events[0].Peer)
	}
}

func TestParseTrafficMatchesMatterIPWhenTopologyMissing(t *testing.T) {
	tx := strings.Join([]string{
		"00:00:01.000",
		"type:UDP len:50 to:0x1234",
		"dst:[fd00:db8:1234:1:20fe:c9a4:37c4:a9ca]:5540",
	}, "\n")
	matter := []MatterDevice{{NodeID: "D", Name: "Detached Switch", ThreadIP: "fd00:db8:1234:1:20fe:c9a4:37c4:a9ca"}}

	events := parseTraffic("", tx, nil, matter)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Note != "Detached Switch" {
		t.Fatalf("note = %q, want Detached Switch", events[0].Note)
	}
	if events[0].Peer != "matter-D" {
		t.Fatalf("peer = %q, want matter-D", events[0].Peer)
	}
}

func TestAddTrafficObservedNodesPromotesMatterTrafficPeer(t *testing.T) {
	nodes := []GraphNode{{ID: "otbr", Label: "OTBR", Kind: "border-router"}}
	links := []GraphLink{}
	events := []TrafficEvent{{
		Direction: "tx",
		Peer:      "matter-D",
		Note:      "Detached Switch",
		Dst:       "fd00:db8:1234:1:20fe:c9a4:37c4:a9ca:5540",
		Type:      "UDP",
		Bytes:     50,
	}}
	matter := []MatterDevice{{NodeID: "D", Name: "Detached Switch", ThreadIP: "fd00:db8:1234:1:20fe:c9a4:37c4:a9ca"}}

	gotNodes, gotLinks := addTrafficObservedNodes(nodes, links, events, matter)

	var observed GraphNode
	for _, n := range gotNodes {
		if n.ID == "matter-D" {
			observed = n
		}
	}
	if observed.ID == "" {
		t.Fatalf("missing observed Matter node: %#v", gotNodes)
	}
	if observed.Label != "Detached Switch" {
		t.Fatalf("label = %q, want Detached Switch", observed.Label)
	}
	if observed.ThreadIP != "fd00:db8:1234:1:20fe:c9a4:37c4:a9ca" {
		t.Fatalf("ThreadIP = %q", observed.ThreadIP)
	}
	if observed.LinkStatus != "traffic-seen" {
		t.Fatalf("LinkStatus = %q, want traffic-seen", observed.LinkStatus)
	}
	if len(gotLinks) != 1 || gotLinks[0].Source != "otbr" || gotLinks[0].Target != "matter-D" {
		t.Fatalf("links = %#v, want OTBR observed link", gotLinks)
	}
}

func TestAddTrafficObservedNodesPromotesUnknownRLOC(t *testing.T) {
	nodes := []GraphNode{{ID: "otbr", Label: "OTBR", Kind: "border-router"}}
	events := []TrafficEvent{{Direction: "rx", Peer: "0x7400", Src: "fd00::1234:5540", RSSI: -79, Bytes: 42}}

	gotNodes, gotLinks := addTrafficObservedNodes(nodes, nil, events, nil)

	var observed GraphNode
	for _, n := range gotNodes {
		if n.ID == "0x7400" {
			observed = n
		}
	}
	if observed.Rloc16 != "0x7400" {
		t.Fatalf("Rloc16 = %q, want 0x7400", observed.Rloc16)
	}
	if observed.ThreadIP != "fd00::1234" {
		t.Fatalf("ThreadIP = %q, want fd00::1234", observed.ThreadIP)
	}
	if len(gotLinks) != 1 || gotLinks[0].RSSI != -79 {
		t.Fatalf("links = %#v", gotLinks)
	}
}

func TestAddMatterInventoryNodesAddsUnmatchedMatterDevices(t *testing.T) {
	nodes := []GraphNode{{ID: "otbr", Label: "OTBR", Kind: "border-router"}}
	matter := []MatterDevice{
		{NodeID: "1", Name: "Window Sensor", ThreadIP: "fd00:db8::1"},
		{NodeID: "D", Name: "Detached Switch"},
		{NodeID: "20", Name: "Air Quality Monitor"},
	}

	gotNodes, gotLinks := addMatterInventoryNodes(nodes, nil, matter)

	byID := map[string]GraphNode{}
	for _, n := range gotNodes {
		byID[n.ID] = n
	}
	if byID["matter-1"].Label != "Window Sensor" || byID["matter-1"].ThreadIP != "fd00:db8::1" {
		t.Fatalf("matter-1 = %#v", byID["matter-1"])
	}
	if byID["matter-D"].Label != "Detached Switch" || byID["matter-D"].LinkStatus != "matter-known" {
		t.Fatalf("matter-D = %#v", byID["matter-D"])
	}
	if byID["matter-20"].Label != "Air Quality Monitor" {
		t.Fatalf("matter-20 = %#v", byID["matter-20"])
	}
	if _, ok := byID["matter-14"]; ok {
		t.Fatalf("hex Matter node id 20 was treated as decimal 20: %#v", byID["matter-14"])
	}
	if len(gotLinks) != 3 {
		t.Fatalf("links = %d, want inventory links for all devices: %#v", len(gotLinks), gotLinks)
	}
}

func TestAddMatterInventoryNodesRenamesTrafficNodeByIP(t *testing.T) {
	nodes := []GraphNode{
		{ID: "otbr", Label: "OTBR", Kind: "border-router"},
		{ID: "0x5005", Label: "0x5005", Kind: "traffic", Rloc16: "0x5005", ThreadIP: "fd00:db8::abcd"},
	}
	matter := []MatterDevice{{NodeID: "2", Name: "Motion Sensor", ThreadIP: "fd00:db8::abcd"}}

	gotNodes, gotLinks := addMatterInventoryNodes(nodes, nil, matter)

	if len(gotNodes) != 2 {
		t.Fatalf("nodes = %d, want existing traffic node renamed, not duplicated: %#v", len(gotNodes), gotNodes)
	}
	if gotNodes[1].Label != "Motion Sensor" {
		t.Fatalf("label = %q, want Matter name", gotNodes[1].Label)
	}
	if gotNodes[1].LinkStatus != "matter-linked" {
		t.Fatalf("LinkStatus = %q, want matter-linked", gotNodes[1].LinkStatus)
	}
	if len(gotLinks) != 0 {
		t.Fatalf("links = %#v, want no inventory link for matched node", gotLinks)
	}
}

package core

import (
	"context"
	"testing"
	"time"

	"agenttroop/internal/clock"
	"agenttroop/internal/store"
	"agenttroop/internal/store/memory"
)

func TestSimulationDeterministic(t *testing.T) {
	in := SimulationInput{Scenario:"chaos", Seed:42, Tasks:1000, Agents:9, FailureRate:.08, ChaosRate:.12, ExpectedCost:1}
	a, err := RunSimulation(in); if err != nil { t.Fatal(err) }
	b, err := RunSimulation(in); if err != nil { t.Fatal(err) }
	if a.StateHash != b.StateHash || a.P95LatencyMS != b.P95LatencyMS || a.StateHash == "" { t.Fatalf("not deterministic: %+v %+v", a, b) }
	if a.Tasks != a.Succeeded+a.Failed || a.LoadGini < 0 || a.LoadGini > 1 { t.Fatalf("bad report: %+v", a) }
}

func TestMarketplaceAndCanary(t *testing.T) {
	clk := clock.NewFake(time.Date(2026,8,27,0,0,0,0,time.UTC)); st := memory.New(); svc := New(st, clk, DefaultConfig())
	for _, a := range []*store.Agent{{ID:"a",Name:"a",Platform:"hermes",Health:"healthy",Capabilities:[]store.Capability{{Skill:"go",Level:.8}}},{ID:"b",Name:"b",Platform:"openclaw",Health:"down",Capabilities:[]store.Capability{{Skill:"go",Level:1}}}} { if err := st.UpsertAgent(context.Background(), a, clk.Now()); err != nil { t.Fatal(err) }; if a.ID == "b" { _ = st.MarkAgentHealth(context.Background(), a.ID, "down") } }
	agents, err := svc.DiscoverAgents(context.Background(), MarketplaceQuery{Skill:"go",HealthyOnly:true}); if err != nil { t.Fatal(err) }
	if len(agents) != 1 || agents[0].ID != "a" { t.Fatalf("agents=%+v", agents) }
	result, err := svc.EvaluateCanary(context.Background(), CanaryInput{ID:"gold-1",VerifierAgentID:"a",ExpectedVerdict:"accepted",ActualVerdict:"accepted",ExpectedScore:.9,ActualScore:.85}); if err != nil { t.Fatal(err) }
	if !result.Match { t.Fatalf("result=%+v", result) }
}

func TestS3RequestSigningAndKMS(t *testing.T) {
	b := S3Blob{Endpoint:"https://minio.example",Bucket:"artifacts",Region:"us-east-1",AccessKey:"key",SecretKey:"secret",KMSKeyID:"kms-1",Now:func() time.Time{return time.Date(2026,8,27,1,2,3,0,time.UTC)}}
	req, err := b.request("PUT", "abcdef0123456789", []byte("hello")); if err != nil { t.Fatal(err) }
	if req.URL.Path != "/artifacts/ab/abcdef0123456789" || req.Header.Get("Authorization") == "" || req.Header.Get("X-Amz-Server-Side-Encryption") != "aws:kms" { t.Fatalf("request=%+v headers=%v", req.URL, req.Header) }
}

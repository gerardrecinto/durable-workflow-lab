// scenarios runs every scenario against a real Temporal dev server and writes
// each run's history twice: as protojson under testdata/ (replay tests read it)
// and as a compact timeline under docs/histories/ (the Pages viewer reads it).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gerardrecinto/durable-workflow-lab/internal/export"
	"github.com/gerardrecinto/durable-workflow-lab/internal/harness"
	"github.com/gerardrecinto/durable-workflow-lab/internal/scenarios"
)

type doc struct {
	Name     string         `json:"name"`
	Title    string         `json:"title"`
	Blurb    string         `json:"blurb"`
	Result   any            `json:"result"`
	Effects  []string       `json:"sideEffects"`
	Calls    map[string]int `json:"calls"`
	Timeline []export.Item  `json:"timeline"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env, err := harness.Start(ctx)
	if err != nil {
		panic(err)
	}
	defer env.Stop()
	_ = os.MkdirAll("testdata", 0o755)
	_ = os.MkdirAll("docs/histories", 0o755)
	var index []map[string]string
	for _, s := range scenarios.All() {
		o, err := s.Run(ctx, env)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", s.Name, err)
			os.Exit(1)
		}
		h, err := export.History(ctx, env.Client, o.WorkflowID, o.RunID)
		if err != nil {
			panic(err)
		}
		raw, _ := export.JSON(h)
		_ = os.WriteFile("testdata/"+s.Name+".json", raw, 0o644)
		calls := map[string]int{}
		for _, op := range []string{"reserve", "charge", "ship", "refund", "release"} {
			calls[op] = o.Provider.Calls(op)
		}
		d := doc{Name: s.Name, Title: s.Title, Blurb: s.Blurb, Result: o.Result, Effects: o.Provider.Order(), Calls: calls, Timeline: export.Timeline(h)}
		b, _ := json.MarshalIndent(d, "", " ")
		_ = os.WriteFile("docs/histories/"+s.Name+".json", b, 0o644)
		index = append(index, map[string]string{"name": s.Name, "title": s.Title})
		fmt.Printf("%-11s status=%-11s events=%-3d calls=%v\n", s.Name, o.Result.Status, len(h.Events), calls)
	}
	b, _ := json.Marshal(index)
	_ = os.WriteFile("docs/histories/index.json", b, 0o644)
}

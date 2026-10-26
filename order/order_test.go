package order_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gerardrecinto/durable-workflow-lab/internal/harness"
	"github.com/gerardrecinto/durable-workflow-lab/internal/scenarios"
	"github.com/gerardrecinto/durable-workflow-lab/order"
)

var env *harness.Env

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var err error
	env, err = harness.Start(ctx)
	if err != nil {
		panic(err)
	}
	code := m.Run()
	env.Stop()
	os.Exit(code)
}

func run(t *testing.T, name string) *scenarios.Outcome {
	t.Helper()
	for _, s := range scenarios.All() {
		if s.Name == name {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			o, err := s.Run(ctx, env)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			return o
		}
	}
	t.Fatalf("no scenario %q", name)
	return nil
}

func TestHappyPath(t *testing.T) {
	o := run(t, "happy")
	if o.Result.Status != "completed" || o.Result.Tracking == "" {
		t.Fatalf("got %+v", o.Result)
	}
	if got := o.Provider.Order(); !reflect.DeepEqual(got, []string{"reserve", "charge", "ship"}) {
		t.Fatalf("side effects %v", got)
	}
}

func TestTransientFailureIsRetriedWithoutWorkflowCode(t *testing.T) {
	o := run(t, "retry")
	if o.Result.Status != "completed" {
		t.Fatalf("got %+v", o.Result)
	}
	if n := o.Provider.Calls("charge"); n != 3 {
		t.Fatalf("charge attempted %d times, want 3", n)
	}
	if o.Provider.ChargesApplied() != 1 {
		t.Fatal("card charged more than once")
	}
}

func TestDeclineIsNotRetriedAndCompensates(t *testing.T) {
	o := run(t, "decline")
	if o.Result.Status != "compensated" || !reflect.DeepEqual(o.Result.Compensated, []string{"release"}) {
		t.Fatalf("got %+v", o.Result)
	}
	if n := o.Provider.Calls("charge"); n != 1 {
		t.Fatalf("a non-retryable decline was attempted %d times", n)
	}
	if !strings.Contains(o.Result.Reason, order.ErrDeclined) {
		t.Fatalf("reason %q", o.Result.Reason)
	}
}

func TestLateFailureUnwindsInReverseOrder(t *testing.T) {
	o := run(t, "ship-fails")
	if !reflect.DeepEqual(o.Result.Compensated, []string{"refund", "release"}) {
		t.Fatalf("compensation order %v", o.Result.Compensated)
	}
	if n := o.Provider.Calls("ship"); n != 5 {
		t.Fatalf("ship attempted %d times, want 5 (max attempts)", n)
	}
}

func TestCancelSignalBeatsTheTimer(t *testing.T) {
	o := run(t, "cancel")
	if o.Result.Status != "cancelled" || !reflect.DeepEqual(o.Result.Compensated, []string{"refund", "release"}) {
		t.Fatalf("got %+v", o.Result)
	}
	if o.Provider.Calls("ship") != 0 {
		t.Fatal("shipped a cancelled order")
	}
}

func TestWorkerLossResumesFromHistory(t *testing.T) {
	o := run(t, "crash")
	if o.Result.Status != "completed" {
		t.Fatalf("got %+v", o.Result)
	}
	if n := o.Provider.Calls("reserve"); n != 1 {
		t.Fatalf("reserve ran %d times; completed steps must not be re-executed", n)
	}
	if n := o.Provider.Calls("charge"); n < 2 {
		t.Fatalf("charge ran %d times; the in-flight attempt should have been retried", n)
	}
	if o.Provider.ChargesApplied() != 1 {
		t.Fatal("idempotency key failed: card charged more than once")
	}
}

// ---- replay and versioning ----

func quiet() log.Logger {
	return log.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func histories(t *testing.T) []string {
	t.Helper()
	files, _ := filepath.Glob("../testdata/*.json")
	if len(files) == 0 {
		t.Skip("run `go run ./cmd/scenarios` first to record histories")
	}
	return files
}

func TestRecordedHistoriesReplayOnCurrentCode(t *testing.T) {
	for _, f := range histories(t) {
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflow(order.OrderWorkflow)
		if err := r.ReplayWorkflowHistoryFromJSONFile(quiet(), f); err != nil {
			t.Fatalf("%s: %v", filepath.Base(f), err)
		}
	}
}

func TestVersionedChangeStillReplaysOldHistories(t *testing.T) {
	for _, f := range histories(t) {
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflowWithOptions(order.OrderWorkflowV2, workflow.RegisterOptions{Name: "OrderWorkflow"})
		if err := r.ReplayWorkflowHistoryFromJSONFile(quiet(), f); err != nil {
			t.Fatalf("%s should replay under GetVersion: %v", filepath.Base(f), err)
		}
	}
}

func TestUnversionedChangeBreaksReplay(t *testing.T) {
	for _, f := range histories(t) {
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflowWithOptions(order.OrderWorkflowUnsafe, workflow.RegisterOptions{Name: "OrderWorkflow"})
		err := r.ReplayWorkflowHistoryFromJSONFile(quiet(), f)
		if err == nil {
			t.Fatalf("%s replayed cleanly; the replayer should reject the unversioned change", filepath.Base(f))
		}
		// The SDK tags non-determinism failures with code TMPRL1100; the wording after it
		// differs by where the mismatch is found, so match the code, not the prose.
		if !strings.Contains(err.Error(), "TMPRL1100") && !strings.Contains(strings.ToLower(err.Error()), "nondeterministic") {
			t.Fatalf("%s: unexpected error %v", filepath.Base(f), err)
		}
	}
}

func TestNewExecutionsOfV2RunTheFraudCheck(t *testing.T) {
	q := env.Queue("v2")
	p := order.NewProvider()
	w, err := env.Worker(q, p, worker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	in := order.Input{OrderID: "o-v2", Amount: 100, CancelWindow: 200 * time.Millisecond}
	run, err := env.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "wf-v2", TaskQueue: q}, "OrderWorkflowV2", in)
	if err != nil {
		t.Fatal(err)
	}
	var res order.Result
	if err := run.Get(ctx, &res); err != nil || res.Status != "completed" {
		t.Fatalf("%v %+v", err, res)
	}
	if p.Calls("fraud") != 1 {
		t.Fatalf("fraud check ran %d times", p.Calls("fraud"))
	}
}

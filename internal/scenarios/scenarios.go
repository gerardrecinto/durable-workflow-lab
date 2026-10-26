// Package scenarios drives the order workflow through the situations durable
// execution is meant for. Tests assert on the outcomes; cmd/scenarios records
// the same runs as timelines for the docs viewer.
package scenarios

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/gerardrecinto/durable-workflow-lab/internal/harness"
	"github.com/gerardrecinto/durable-workflow-lab/order"
)

type Outcome struct {
	Name       string
	Title      string
	Blurb      string
	WorkflowID string
	RunID      string
	Result     order.Result
	Provider   *order.Provider
	Notes      []string
}

type Spec struct {
	Name  string
	Title string
	Blurb string
	Run   func(ctx context.Context, e *harness.Env) (*Outcome, error)
}

func base(id string) order.Input {
	return order.Input{OrderID: id, Amount: 4200, CancelWindow: 300 * time.Millisecond}
}

func start(ctx context.Context, e *harness.Env, q, id string, in order.Input, wf any) (client.WorkflowRun, error) {
	return e.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: q}, wf, in)
}

func finish(ctx context.Context, run client.WorkflowRun, o *Outcome) (*Outcome, error) {
	o.WorkflowID, o.RunID = run.GetID(), run.GetRunID()
	if err := run.Get(ctx, &o.Result); err != nil {
		return o, err
	}
	return o, nil
}

func simple(name, title, blurb string, prep func(p *order.Provider, in *order.Input)) Spec {
	return Spec{Name: name, Title: title, Blurb: blurb, Run: func(ctx context.Context, e *harness.Env) (*Outcome, error) {
		q := e.Queue(name)
		p := order.NewProvider()
		in := base("o-" + name)
		prep(p, &in)
		w, err := e.Worker(q, p, worker.Options{})
		if err != nil {
			return nil, err
		}
		defer w.Stop()
		run, err := start(ctx, e, q, "wf-"+name, in, order.OrderWorkflow)
		if err != nil {
			return nil, err
		}
		return finish(ctx, run, &Outcome{Name: name, Title: title, Blurb: blurb, Provider: p})
	}}
}

func All() []Spec {
	return []Spec{
		simple("happy", "Happy path", "Reserve, charge, wait out the cancel window on a durable timer, ship.",
			func(p *order.Provider, in *order.Input) {}),
		simple("retry", "Transient failure is retried", "The payment provider fails twice. Temporal retries the activity with backoff; the workflow code contains no retry loop.",
			func(p *order.Provider, in *order.Input) { p.FailNext("charge", 2) }),
		simple("decline", "Declined card triggers compensation", "A decline is a non-retryable error. The saga releases the inventory reservation that was already made.",
			func(p *order.Provider, in *order.Input) { p.Decline(in.OrderID) }),
		simple("ship-fails", "Late failure unwinds earlier steps", "Shipping keeps failing and exhausts its retries. Compensations run in reverse order: refund, then release.",
			func(p *order.Provider, in *order.Input) { p.FailNext("ship", 99) }),
		{Name: "cancel", Title: "Cancel signal during the window", Blurb: "The customer cancels while the durable timer is running. The signal wins the race, the timer is cancelled and the charge is refunded.",
			Run: func(ctx context.Context, e *harness.Env) (*Outcome, error) {
				q := e.Queue("cancel")
				p := order.NewProvider()
				in := base("o-cancel")
				in.CancelWindow = 5 * time.Second
				w, err := e.Worker(q, p, worker.Options{})
				if err != nil {
					return nil, err
				}
				defer w.Stop()
				run, err := start(ctx, e, q, "wf-cancel", in, order.OrderWorkflow)
				if err != nil {
					return nil, err
				}
				if err := waitStatus(ctx, e, run, "cancel-window"); err != nil {
					return nil, err
				}
				if err := e.Client.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), order.SignalCancel, nil); err != nil {
					return nil, err
				}
				return finish(ctx, run, &Outcome{Name: "cancel", Title: "Cancel signal during the window", Blurb: "", Provider: p})
			}},
		{Name: "crash", Title: "Worker dies mid-activity, another resumes", Blurb: "The first worker is stopped while a charge call is in flight. A second worker picks the workflow up from its history: Reserve is not repeated, the charge is retried and the idempotency key keeps it to one charge.",
			Run: func(ctx context.Context, e *harness.Env) (*Outcome, error) {
				q := e.Queue("crash")
				p := order.NewProvider()
				p.ChargeGate = make(chan struct{})
				p.ChargeStarted = make(chan struct{}, 1)
				in := base("o-crash")
				in.ActivityTimeout = time.Second
				w1, err := e.Worker(q, p, worker.Options{WorkerStopTimeout: 100 * time.Millisecond})
				if err != nil {
					return nil, err
				}
				run, err := start(ctx, e, q, "wf-crash", in, order.OrderWorkflow)
				if err != nil {
					return nil, err
				}
				select {
				case <-p.ChargeStarted:
				case <-time.After(20 * time.Second):
					return nil, fmt.Errorf("charge never started")
				}
				w1.Stop() // the "crash": the in-flight charge never reports back
				o := &Outcome{Name: "crash", Title: "Worker dies mid-activity, another resumes", Provider: p, Notes: []string{"worker 1 stopped while charge was in flight", "worker 2 started"}}
				w2, err := e.Worker(q, p, worker.Options{})
				if err != nil {
					return nil, err
				}
				defer w2.Stop()
				defer p.OpenGate() // lets the abandoned attempt finish; the idempotency key makes it a no-op
				return finish(ctx, run, o)
			}},
	}
}

func waitStatus(ctx context.Context, e *harness.Env, run client.WorkflowRun, want string) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		v, err := e.Client.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), order.QueryStatus)
		if err == nil {
			var s string
			if v.Get(&s) == nil && s == want {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("workflow never reached status %q", want)
}

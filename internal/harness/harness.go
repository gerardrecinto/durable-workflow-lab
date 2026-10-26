// Package harness starts a real Temporal dev server (the Temporal CLI binary,
// downloaded on first use) and runs workers against it. Nothing here is mocked:
// tests and scenarios exercise the same server, history and replay machinery
// that production Temporal uses.
package harness

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gerardrecinto/durable-workflow-lab/order"
)

type Env struct {
	Server *testsuite.DevServer
	Client client.Client
	n      atomic.Int64
}

func Start(ctx context.Context) (*Env, error) {
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{},
	})
	if err != nil {
		return nil, fmt.Errorf("start dev server: %w", err)
	}
	return &Env{Server: srv, Client: srv.Client()}, nil
}

func (e *Env) Stop() {
	e.Client.Close()
	_ = e.Server.Stop()
}

// Queue returns a fresh task queue name so tests never see each other's work.
func (e *Env) Queue(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, e.n.Add(1))
}

// Worker starts a worker for the order workflows on a queue.
func (e *Env) Worker(queue string, p *order.Provider, opts worker.Options) (worker.Worker, error) {
	w := worker.New(e.Client, queue, opts)
	w.RegisterWorkflow(order.OrderWorkflow)
	w.RegisterWorkflowWithOptions(order.OrderWorkflowV2, registerAs("OrderWorkflowV2"))
	w.RegisterActivity(&order.Activities{P: p})
	return w, w.Start()
}

func registerAs(name string) workflow.RegisterOptions { return workflow.RegisterOptions{Name: name} }

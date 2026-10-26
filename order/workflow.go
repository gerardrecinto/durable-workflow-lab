// Package order is a saga-style order workflow on Temporal: reserve stock,
// charge the card, wait out a cancellation window, ship. If a later step
// fails or the customer cancels, earlier steps are undone in reverse order.
package order

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Input struct {
	OrderID string
	Amount  int
	// CancelWindow is a durable timer during which a cancel signal is honoured.
	CancelWindow time.Duration
	// ActivityTimeout bounds one activity attempt. Short in tests so a lost
	// worker is noticed quickly.
	ActivityTimeout time.Duration
}

type Result struct {
	Status      string // completed | compensated | cancelled
	Reason      string
	ChargeID    string
	Tracking    string
	Compensated []string
}

const (
	SignalCancel = "cancel"
	QueryStatus  = "status"
)

func (in Input) timeout() time.Duration {
	if in.ActivityTimeout == 0 {
		return 10 * time.Second
	}
	return in.ActivityTimeout
}

func activityCtx(ctx workflow.Context, in Input) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: in.timeout(),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        100 * time.Millisecond,
			BackoffCoefficient:     2,
			MaximumInterval:        time.Second,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{ErrDeclined},
		},
	})
}

// OrderWorkflow must be deterministic: no wall clock, no random numbers, no
// goroutines or map iteration order, no direct I/O. Everything that touches the
// world goes through an activity.
func OrderWorkflow(ctx workflow.Context, in Input) (Result, error) {
	return orderFlow(ctx, in, fraudNone)
}

func orderFlow(ctx workflow.Context, in Input, fraud fraudMode) (Result, error) {
	var a *Activities
	status := "started"
	if err := workflow.SetQueryHandler(ctx, QueryStatus, func() (string, error) { return status, nil }); err != nil {
		return Result{}, err
	}
	actx := activityCtx(ctx, in)
	var res Result
	type comp struct {
		name string
		run  func(workflow.Context) error
	}
	var stack []comp
	compensate := func(reason string) Result {
		// A disconnected context keeps compensations running even if the
		// workflow itself was cancelled.
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		dctx = activityCtx(dctx, in)
		for i := len(stack) - 1; i >= 0; i-- {
			if err := stack[i].run(dctx); err == nil {
				res.Compensated = append(res.Compensated, stack[i].name)
			}
		}
		res.Reason = reason
		return res
	}

	switch fraud {
	case fraudUnversioned:
		// BUG on purpose: adds a command to history that old executions never had.
		var ok bool
		if err := workflow.ExecuteActivity(actx, a.FraudCheck, in.OrderID).Get(ctx, &ok); err != nil {
			return res, err
		}
	case fraudVersioned:
		// Old executions replay as DefaultVersion and skip the check; new ones run it.
		if workflow.GetVersion(ctx, "add-fraud-check", workflow.DefaultVersion, 1) == 1 {
			var ok bool
			if err := workflow.ExecuteActivity(actx, a.FraudCheck, in.OrderID).Get(ctx, &ok); err != nil {
				return res, err
			}
		}
	}

	status = "reserving"
	var reservationID string
	if err := workflow.ExecuteActivity(actx, a.Reserve, in).Get(ctx, &reservationID); err != nil {
		status = "failed"
		res.Status, res.Reason = "compensated", "reserve failed: "+err.Error()
		return res, nil
	}
	stack = append(stack, comp{"release", func(c workflow.Context) error {
		return workflow.ExecuteActivity(c, a.Release, reservationID).Get(c, nil)
	}})

	status = "charging"
	var chargeID string
	idem := in.OrderID + ":" + workflow.GetInfo(ctx).WorkflowExecution.RunID
	if err := workflow.ExecuteActivity(actx, a.Charge, ChargeInput{in.OrderID, in.Amount, idem}).Get(ctx, &chargeID); err != nil {
		status = "compensating"
		res = compensate(describe(err))
		res.Status = "compensated"
		status = "compensated"
		return res, nil
	}
	res.ChargeID = chargeID
	stack = append(stack, comp{"refund", func(c workflow.Context) error {
		return workflow.ExecuteActivity(c, a.Refund, chargeID).Get(c, nil)
	}})

	status = "cancel-window"
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancel)
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	timer := workflow.NewTimer(timerCtx, in.CancelWindow)
	cancelled := false
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) { c.Receive(ctx, nil); cancelled = true; cancelTimer() })
	sel.AddFuture(timer, func(f workflow.Future) {})
	sel.Select(ctx)
	if cancelled {
		status = "compensating"
		res = compensate("cancelled by customer")
		res.Status = "cancelled"
		status = "cancelled"
		return res, nil
	}

	status = "shipping"
	if err := workflow.ExecuteActivity(actx, a.Ship, in.OrderID).Get(ctx, &res.Tracking); err != nil {
		status = "compensating"
		res = compensate("ship failed: " + describe(err))
		res.Status = "compensated"
		status = "compensated"
		return res, nil
	}
	status = "completed"
	res.Status = "completed"
	return res, nil
}

func describe(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Type() + ": " + app.Message()
	}
	return err.Error()
}

type fraudMode int

const (
	fraudNone fraudMode = iota
	fraudVersioned
	fraudUnversioned
)

// OrderWorkflowV2 adds a fraud check safely, behind workflow.GetVersion.
func OrderWorkflowV2(ctx workflow.Context, in Input) (Result, error) {
	return orderFlow(ctx, in, fraudVersioned)
}

// OrderWorkflowUnsafe adds the same check without a version marker. Replaying
// an old history against it fails with a non-determinism error. It exists to
// prove that, and must never be deployed.
func OrderWorkflowUnsafe(ctx workflow.Context, in Input) (Result, error) {
	return orderFlow(ctx, in, fraudUnversioned)
}

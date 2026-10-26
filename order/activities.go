package order

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
)

// ErrDeclined is returned when the payment provider refuses the card. It is
// marked non-retryable: retrying a decline would only repeat it.
const ErrDeclined = "PaymentDeclined"

// Provider stands in for the external systems an order touches (inventory,
// payments, carrier). It counts every call so tests can show what Temporal
// retried and what it did not, and it enforces idempotency keys the way a real
// payment API does.
type Provider struct {
	mu sync.Mutex

	calls       map[string]int
	reservation map[string]string
	charges     map[string]string // idempotency key -> charge id
	refunded    map[string]bool
	released    map[string]bool
	failNext    map[string]int
	declined    map[string]bool
	Log         []string

	// ChargeGate, when set, makes the first Charge call block until it is
	// closed. Tests use it to hold an activity in flight while the worker goes away.
	ChargeGate    chan struct{}
	gateTaken     bool
	ChargeStarted chan struct{}
}

func NewProvider() *Provider {
	return &Provider{
		calls: map[string]int{}, reservation: map[string]string{}, charges: map[string]string{},
		refunded: map[string]bool{}, released: map[string]bool{}, failNext: map[string]int{}, declined: map[string]bool{},
	}
}

func (p *Provider) FailNext(op string, n int) { p.mu.Lock(); p.failNext[op] = n; p.mu.Unlock() }
func (p *Provider) Decline(orderID string)    { p.mu.Lock(); p.declined[orderID] = true; p.mu.Unlock() }
func (p *Provider) Calls(op string) int       { p.mu.Lock(); defer p.mu.Unlock(); return p.calls[op] }
func (p *Provider) ChargesApplied() int       { p.mu.Lock(); defer p.mu.Unlock(); return len(p.charges) }
func (p *Provider) Released(id string) bool   { p.mu.Lock(); defer p.mu.Unlock(); return p.released[id] }
func (p *Provider) Refunded(id string) bool   { p.mu.Lock(); defer p.mu.Unlock(); return p.refunded[id] }
func (p *Provider) Order() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.Log...)
}

func (p *Provider) enter(op string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[op]++
	if p.failNext[op] > 0 {
		p.failNext[op]--
		return fmt.Errorf("%s: provider unavailable (injected)", op)
	}
	return nil
}

func (p *Provider) note(s string) { p.mu.Lock(); p.Log = append(p.Log, s); p.mu.Unlock() }

type Activities struct{ P *Provider }

func (a *Activities) Reserve(ctx context.Context, in Input) (string, error) {
	if err := a.P.enter("reserve"); err != nil {
		return "", err
	}
	a.P.mu.Lock()
	id := "res-" + in.OrderID
	a.P.reservation[in.OrderID] = id
	a.P.mu.Unlock()
	a.P.note("reserve")
	return id, nil
}

func (a *Activities) Release(ctx context.Context, reservationID string) error {
	if err := a.P.enter("release"); err != nil {
		return err
	}
	a.P.mu.Lock()
	a.P.released[reservationID] = true
	a.P.mu.Unlock()
	a.P.note("release")
	return nil
}

type ChargeInput struct {
	OrderID string
	Amount  int
	// IdemKey lets the provider ignore a repeat of the same charge. Activities
	// run at least once, so any side effect needs one.
	IdemKey string
}

func (a *Activities) Charge(ctx context.Context, in ChargeInput) (string, error) {
	if err := a.P.enter("charge"); err != nil {
		return "", err
	}
	if a.P.ChargeStarted != nil {
		select {
		case a.P.ChargeStarted <- struct{}{}:
		default:
		}
	}
	if g := a.P.takeGate(); g != nil {
		// Only the first attempt blocks, and it ignores ctx: this models a
		// process that dies mid-call and never reports a result.
		<-g
	}
	a.P.mu.Lock()
	defer a.P.mu.Unlock()
	if a.P.declined[in.OrderID] {
		return "", temporal.NewNonRetryableApplicationError("card declined", ErrDeclined, errors.New("insufficient funds"))
	}
	if id, ok := a.P.charges[in.IdemKey]; ok {
		return id, nil // duplicate delivery of the same charge
	}
	id := "ch-" + in.IdemKey
	a.P.charges[in.IdemKey] = id
	a.P.Log = append(a.P.Log, "charge")
	return id, nil
}

func (a *Activities) Refund(ctx context.Context, chargeID string) error {
	if err := a.P.enter("refund"); err != nil {
		return err
	}
	a.P.mu.Lock()
	a.P.refunded[chargeID] = true
	a.P.mu.Unlock()
	a.P.note("refund")
	return nil
}

func (a *Activities) Ship(ctx context.Context, orderID string) (string, error) {
	if err := a.P.enter("ship"); err != nil {
		return "", err
	}
	a.P.note("ship")
	return "trk-" + orderID, nil
}

// FraudCheck exists only for the workflow versioning example.
func (a *Activities) FraudCheck(ctx context.Context, orderID string) (bool, error) {
	if err := a.P.enter("fraud"); err != nil {
		return false, err
	}
	a.P.note("fraud")
	return true, nil
}

var _ = time.Second

// OpenGate releases any Charge call blocked on ChargeGate. Safe to call twice.
func (p *Provider) OpenGate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ChargeGate != nil {
		select {
		case <-p.ChargeGate:
		default:
			close(p.ChargeGate)
		}
	}
}

func (p *Provider) takeGate() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ChargeGate == nil || p.gateTaken {
		return nil
	}
	p.gateTaken = true
	return p.ChargeGate
}

// Package export turns a real Temporal workflow history into a small JSON
// timeline for the docs viewer, and into the protojson format that
// WorkflowReplayer reads.
package export

import (
	"context"
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
)

type Item struct {
	ID     int64  `json:"id"`
	Type   string `json:"type"`
	Ms     int64  `json:"ms"`
	Detail string `json:"detail,omitempty"`
}

func History(ctx context.Context, c client.Client, id, runID string) (*historypb.History, error) {
	it := c.GetWorkflowHistory(ctx, id, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	h := &historypb.History{}
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			return nil, err
		}
		h.Events = append(h.Events, ev)
	}
	return h, nil
}

func JSON(h *historypb.History) ([]byte, error) {
	return protojson.MarshalOptions{Indent: "  "}.Marshal(h)
}

func Timeline(h *historypb.History) []Item {
	if len(h.Events) == 0 {
		return nil
	}
	t0 := h.Events[0].GetEventTime().AsTime()
	acts := map[int64]string{}
	var out []Item
	for _, e := range h.Events {
		it := Item{ID: e.GetEventId(), Type: strings.TrimPrefix(e.GetEventType().String(), "EVENT_TYPE_"), Ms: e.GetEventTime().AsTime().Sub(t0).Milliseconds()}
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			a := e.GetActivityTaskScheduledEventAttributes()
			acts[e.GetEventId()] = a.GetActivityType().GetName()
			it.Detail = a.GetActivityType().GetName()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			a := e.GetActivityTaskStartedEventAttributes()
			it.Detail = fmt.Sprintf("%s attempt %d", acts[a.GetScheduledEventId()], a.GetAttempt())
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			it.Detail = acts[e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()]
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			a := e.GetActivityTaskFailedEventAttributes()
			it.Detail = acts[a.GetScheduledEventId()] + ": " + a.GetFailure().GetMessage()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			it.Detail = acts[e.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()] + ": timed out"
		case enumspb.EVENT_TYPE_TIMER_STARTED:
			it.Detail = e.GetTimerStartedEventAttributes().GetStartToFireTimeout().AsDuration().String()
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			it.Detail = e.GetWorkflowExecutionSignaledEventAttributes().GetSignalName()
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			it.Detail = e.GetWorkflowTaskFailedEventAttributes().GetFailure().GetMessage()
		}
		out = append(out, it)
	}
	return out
}

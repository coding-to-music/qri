package event

import (
	"context"
	"fmt"
)

func Example() {
	const (
		ETMainSaidHello   = Type("main:SaidHello")
		ETMainOpSucceeded = Type("main:OperationSucceeded")
		ETMainOpFailed    = Type("main:OperationFailed")
	)

	ctx, done := context.WithCancel(context.Background())
	defer done()

	bus := NewBus(ctx)

	makeDoneHandler := func(label string) Handler {
		return func(ctx context.Context, t Type, payload interface{}) error {
			fmt.Printf("%s handler called\n", label)
			return nil
		}
	}

	bus.Subscribe(makeDoneHandler("first"), ETMainSaidHello, ETMainOpSucceeded)
	bus.Subscribe(makeDoneHandler("second"), ETMainSaidHello)
	bus.Subscribe(makeDoneHandler("third"), ETMainSaidHello)

	bus.Publish(ctx, ETMainSaidHello, "hello")
	bus.Publish(ctx, ETMainOpSucceeded, "operation worked!")

	// Output: first handler called
	// second handler called
	// third handler called
	// first handler called
}

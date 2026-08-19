import queue

## FIXME this feels like a bit of a hack, bridging push based rxpy and pull based generator funcs
#   I'd probably prefer to work out if there's the equivalent of Project Reactor Backpressure

def observable_to_generator(observable):
    q = queue.Queue()
    DONE = object()

    def on_next(item):
        q.put(("next", item))

    def on_error(err):
        q.put(("error", err))

    def on_completed():
        q.put(("done", DONE))

    disposable = observable.subscribe(
        on_next=on_next, on_error=on_error, on_completed=on_completed
    )

    try:
        while True:
            kind, payload = q.get()
            if kind == "next":
                yield payload
            elif kind == "error":
                raise payload
            else:
                return
    finally:
        disposable.dispose()
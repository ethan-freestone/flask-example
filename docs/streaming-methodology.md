# Streaming Data in Flask: From RxPY to Native Generators

> NOTE! This document is (currently) completely AI generated and awaiting review and rewrites. It serves mainly as a reminder of the journey steps currently

This document captures the architectural evolution of the GOKb data ingestion streaming endpoint. It tracks the journey from an initial reactive (RxPY) implementation to a native Python generator approach, highlighting the trade-offs of bridging reactive streams with Flask's synchronous WSGI environment.

## The Goal
The objective was to build a robust, memory-efficient data pipeline that could fetch paginated records from an external API (GOKb), batch process them into a PostgreSQL database, and stream real-time progress updates to the frontend via Server-Sent Events (SSE). 

## The First Attempt: Bringing Reactive Streams to Python
Coming from environments where reactive streams (like Project Reactor or Spring WebFlux) are the gold standard for non-blocking data pipelines, my first instinct was to reach for **RxPY**. 

The code was elegant and declarative. I set up an observable stream that paginated the HTTP requests, piped it through `ops.buffer_with_count()` to chunk the records into database-friendly sizes, and mapped it to a database commit function.

**The Catch: The Push/Pull Impedance Mismatch**
The friction started when trying to hook the RxPY pipeline into Flask's SSE endpoint. 
* **RxPY is push-based:** It fires an `on_next` callback as soon as data is ready.
* **Flask's SSE is pull-based:** It relies on a standard Python `yield` generator, waiting to request the next chunk of data.

You can't seamlessly `yield` from inside an `on_next` callback. To make them talk to each other, I had to build a concurrency bridge: running the RxPY pipeline in a background `ThreadPoolExecutor` and pushing the results into a thread-safe `queue.Queue`, which the Flask generator then blocked on and consumed.

While this worked, it felt risky for a production WSGI environment. Flask (typically running on Gunicorn) relies on a limited pool of synchronous worker threads. Tying up a request thread to wait on a queue, while simultaneously spawning background threads for the reactive stream, is a recipe for thread exhaustion and memory leaks if clients disconnect unexpectedly.

## The Pivot: Embracing Native Python Generators
Instead of forcing a push-based reactive library into a pull-based synchronous framework, I realized I could achieve the exact same memory efficiency and lazy evaluation by leaning into standard Python.

By dropping RxPY and using native Python generators alongside `itertools.batched`, the entire pipeline became pull-based:
1. The HTTP fetcher acts as a lazy generator, yielding one record at a time.
2. `batched()` pulls exactly 500 records from the fetcher, pausing the HTTP stream.
3. The Flask route processes the batch, commits it to the database, and `yield`s the SSE update directly to the client.

This approach eliminated the need for thread pools and queues entirely. It provides "free" backpressure through Python's native iterator protocol, keeps memory consumption perfectly flat, and respects Flask's WSGI architecture. (If this were an ASGI framework like FastAPI, I'd use native `async` generators, but for Flask, pure sync generators are the most bulletproof approach).

---

## Performance & Complexity Analysis

*(Currently running the RxPY pipeline to establish a baseline. I will update the metrics below once the Native Generator refactor is complete to compare the two approaches side-by-side.)*

| Metric | RxPY + Queue Bridge | Native Generators (`itertools`) |
| :--- | :--- | :--- |
| **Execution Time (10k records)** | *[To be populated]* | *[To be populated]* |
| **Peak Memory Usage** | *[To be populated]* | *[To be populated]* |
| **Code Complexity (Mental Overhead)** | High (Requires manual thread and queue management) | Low (Standard procedural loop) |
| **Resilience to Client Disconnects** | Brittle (Requires catching `GeneratorExit` to `.dispose()` background threads) | Robust (Generator simply stops pulling data) |

**Initial Thoughts:**
I expect the execution times to be roughly identical, as the bottleneck is fundamentally I/O (network requests and database inserts). However, the real victory of the native approach will be the dramatic reduction in complexity and the elimination of zombie-thread risks.
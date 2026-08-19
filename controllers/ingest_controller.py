import time
import json
from itertools import batched

from flask import Blueprint, jsonify, Response, stream_with_context, current_app

from services.gokb_client_service import stream_http_records
from services.ingest_service import run_test_ingest, process_and_commit_batch, run_scroll_ingest
from services.titles_service import nuke_titles, nuke_titles_chunked

# TODO look into pydantic for validation and type checking

ingest_bp = Blueprint("ingest", __name__, url_prefix="/ingest")

@ingest_bp.route("/run_test", methods=["POST"])
def run_test():
    result = run_test_ingest()

    return jsonify({
        "status": "success",
        "created_records": result.get("ingest_count"),
        "elapsed_time_ns": result.get("execution_time")
    }), 201

@ingest_bp.route("/gokb", methods=["POST"])
def run_gokb():
    output = run_scroll_ingest()

    return jsonify({
        "status": "success",
        "created_records": output.get("ingest_count"),
        "elapsed_time_ns": output.get("execution_time")
    }), 201


@ingest_bp.route("/gokb_stream", methods=["POST"])
def stream_gokb_ingest():
    def generate():
        start_time = time.perf_counter_ns()
        total_ingested = 0
        batch_count = 0
        app = current_app._get_current_object()

        # batched() pulls exactly 500 items from the lazy stream,
        # pausing the HTTP fetch until needed. No queues required.
        for batch in batched(stream_http_records(5000), 1000):
            count = process_and_commit_batch(batch, app)

            total_ingested += count
            batch_count += 1

            yield f"data: {json.dumps({
                'status': 'processing',
                'batch': batch_count,
                'processed_in_batch': count,
                'total_ingested': total_ingested,
                'execution_time': time.perf_counter_ns() - start_time
            })}\n\n"

        yield f"data: {json.dumps({
            'status': 'complete',
            'total_ingested': total_ingested,
            'execution_time': time.perf_counter_ns() - start_time
        })}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@ingest_bp.route("/nuke", methods=["POST"])
def nuke_db():
    result = nuke_titles()

    return jsonify({
        "status": "success",
        "nuke_records": result.get("deleted_count"),
        "elapsed_time_ns": result.get("execution_time"),
    }), 200

@ingest_bp.route("/nuke_stream", methods=["POST"])
def nuke_db_stream():
    result = nuke_titles_chunked()

    return jsonify({
        "status": "success",
        "nuke_records": result.get("deleted_count"),
        "elapsed_time_ns": result.get("execution_time"),
    }), 200
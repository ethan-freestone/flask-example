import time
import json

from flask import Blueprint, jsonify, Response, stream_with_context, current_app
from reactivex import operators as ops

from services.ingest_service import run_test_ingest, run_scroll_ingest, stream_scroll_ingest
from services.titles_service import nuke_titles, nuke_titles_chunked
from services.observable_service import observable_to_generator

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

        observable = stream_scroll_ingest(2000, 5000).pipe(
            ops.map(lambda batch_and_count: batch_and_count)  # whatever shape you need
        )

        for batch, count in observable_to_generator(observable):
            nonlocal_total = total_ingested + count
            total_ingested = nonlocal_total
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
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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
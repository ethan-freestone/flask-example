import time

from flask import Blueprint, jsonify
from services.ingest_service import run_test_ingest
from services.titles_service import nuke_titles, nuke_titles_chunked

# TODO look into pydantic for validation and type checking

ingest_bp = Blueprint("ingest", __name__, url_prefix="/ingest")

@ingest_bp.route("/run_test", methods=["POST"])
def run_test_ingest():
    start = time.perf_counter_ns()
    count = run_test_ingest()
    end = time.perf_counter_ns()

    return jsonify({
        "status": "success",
        "crated_records": count,
        "elapsed_time_ns": end - start
    }), 201

@ingest_bp.route("/nuke", methods=["POST"])
def nuke_db():
    start = time.perf_counter_ns()
    count = nuke_titles()
    end = time.perf_counter_ns()

    return jsonify({
        "status": "success",
        "nuke_records": count,
        "elapsed_time_ns": end - start
    }), 200

@ingest_bp.route("/nuke_stream", methods=["POST"])
def nuke_db_stream():
    start = time.perf_counter_ns()
    count = nuke_titles_chunked()
    end = time.perf_counter_ns()

    return jsonify({
        "status": "success",
        "nuke_records": count,
        "elapsed_time_ns": end - start
    }), 200
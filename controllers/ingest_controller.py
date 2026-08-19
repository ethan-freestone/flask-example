import time

from flask import Blueprint, jsonify
from services.ingest_service import run_test_ingest, run_scroll_ingest
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
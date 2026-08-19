from flask import request, Blueprint, jsonify
from extensions import db # This is the centralised DB connection
from models.tipp import Title, Identifier

# TODO look into pydantic for validation and type checking

ingest_bp = Blueprint("ingest", __name__, url_prefix="/ingest")

@ingest_bp.route("/ingest", methods=["POST"])
def run_ingest():
    title = Title(
        title="test",
        publication_type="Serial",
        raw_record={ 'test': 1234, 'other_key': 'abcd' }
    )

    title.identifiers.append(Identifier(
        id_type="ISSN",
        id_value="1234-5678"
    ))

    db.session.add(title)
    db.session.commit()

    return jsonify({
        "status": "success",
        "ingest_count": 1
    }), 201
from flask import Blueprint, jsonify, request
from extensions import db # This is the centralised DB connection
from models.tipp import Title, Identifier

## LEarning how to translate spring/MN patterns to Flask
# url_prefix behaves like Micronaut's @Controller("/tipp")
tipp_bp = Blueprint("tipp", __name__, url_prefix="/tipp")

@tipp_bp.route("", methods=["GET"])
def get_all():
    titles = db.session.scalars(db.select(Title)).all()
    return jsonify([t.to_dict() for t in titles]), 200

# Typing DIRECT in route params
@tipp_bp.route("/<int:title_id>", methods=["GET"])
def get_one(title_id):
    title = db.session.get(Title, title_id)
    if not title:
        return jsonify({"error": "Title not found"}), 404
    return jsonify(title.to_dict()), 200
from flask import Flask

from controllers.ingest_controller import ingest_bp
from extensions import db, migrate
from controllers.tipp_controller import tipp_bp

# Boilerplate mostly, but this creates app with SQL connection
# Not sure exactly why this isn't in some config type file rn
def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        "postgresql+psycopg://flask_user:flask_password@localhost:5432/flask_db"
    )


    db.init_app(app)
    migrate.init_app(app, db) ## Bind migrations from flask-migrate

    # Register controllers (Blueprints)
    app.register_blueprint(tipp_bp)
    app.register_blueprint(ingest_bp)


    # Alembic (Via Flask migrate) handles schema, so no need for create_all

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=5000)
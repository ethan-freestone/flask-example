from flask import Flask
from extensions import db
from controllers.tipp_controller import tipp_bp

# Boilerplate mostle, but this creates app with SQL connection
# Not sure exactly why this isn't in some config type file rn
def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        "postgresql+psycopg://flask_user:flask_password@localhost:5432/flask_db"
    )


    db.init_app(app)

    # Register controllers (Blueprints)
    app.register_blueprint(tipp_bp)

    with app.app_context():
        db.create_all()

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=5000)
from flask import Flask, jsonify
import httpx

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/data', methods=['GET'])
def get_data():
    # Using a context manager ensures connections are pooled and closed properly
    with httpx.Client() as client:
        response = client.get('https://gokb.org/gokb/api/find?componentType=TIPP')
        response.raise_for_status()
        return jsonify(response.json())

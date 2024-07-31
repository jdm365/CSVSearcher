from flask import Flask, request, jsonify
from flask_cors import CORS

from time import perf_counter
import os

from er_interactive.search import InteractiveSearcher

app = Flask(__name__)
CORS(app)

def upper(x: str):
    if x is None or x == '':
        return None
    return x.upper()

def lower(x: str):
    if x is None or x == '':
        return None
    return x.lower()

def to_float_or_none(x: str):
    if x is None or x == '':
        return None
    return float(x)

def to_int_or_none(x: str):
    if x is None or x == '':
        return None
    return int(x)

@app.route('/healthcheck', methods=['HEAD'])
def healthcheck():
    return '', 200

@app.route('/columns', methods=['GET'])
def columns():
    columns = [
            'name',
            'address',
            'city',
            'country',
            'score',
            'confidence',
            'lat',
            'lon',
            'uuid',
            'distance_km',
            'num_assets',
            'num_subsidiaries',
            'ultimate_parent_uuid',
            ]

    return jsonify({'columns': columns})

@app.route('/search', methods=['GET'])
def search():
    init = perf_counter()

    event = {
            'uuid': lower(request.args.get("query_uuid")),
            'name': upper(request.args.get("query_name")),
            'address': upper(request.args.get("query_address")),
            'lat': to_float_or_none(request.args.get("lat")),
            'lon': to_float_or_none(request.args.get("lon")),
            'city': upper(request.args.get("city")),
            'country_iso3': upper(request.args.get("country")),
            'max_dist_km': to_float_or_none(request.args.get("max_dist_km", 1000.0)),
            'company_search': request.args.get("company_search", "false").lower() == "true",
            'bbox': [to_float_or_none(x) for x in request.args.get("bbox", ",,,").split(",")],
            'limit': to_int_or_none(request.args.get("limit", 25)),
            }
    print(event.get("city"))
    results = searcher.query(**event)

    ## Make everything a string
    results = [{k: str(v) for k, v in x.items()} for x in results]

    time_taken_ms = int(1e3) * (perf_counter() - init)

    return jsonify({
        'results': results, 
        'time_taken_ms': time_taken_ms,
        'ping_time_ms': ping_time_ms
        })

def launch_app():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

    INDEX_FILENAME = os.path.join(CURRENT_DIR, "index.html")
    os.system(f"open {INDEX_FILENAME}")

    global searcher, ping_time_ms

    searcher = InteractiveSearcher()

    times = []
    for _ in range(5):
        init = perf_counter()
        searcher.get_ping_time_ms()
        times.append(perf_counter() - init)

    ping_time_ms = int(1e3 * sum(times) / len(times))
    print(f"ping_time_ms: {ping_time_ms}")

    app.run()

if __name__ == '__main__':
    launch_app()

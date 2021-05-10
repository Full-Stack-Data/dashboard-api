import os
import psycopg2
from flask import jsonify

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DB'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PW')
    )
    
    cur = conn.cursor()
    cur.execute(f"SELECT date, SUM(sessions), SUM(pageviews), SUM(pageviews_w_timespent), SUM(tot_time) FROM overall GROUP BY date ORDER BY date;")
    records = cur.fetchall()
    resp = []
    for row in records:
        resp.append({
            "date": row[0],
            "sessions": int(row[1]),
            "pageviews": int(row[2]),
            "pageviews_w_timespent": int(row[3]),
            "tot_time": int(row[3])
        })

    if request_json and 'message' in request_json:
        return request_json['message']
    else:
        return jsonify(resp)

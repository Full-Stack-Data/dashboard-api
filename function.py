import os
import psycopg2
import json
from datetime import datetime

def make_connection():
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DB'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PW')
    )
    return conn

def convert_seconds_to_minutes(s):
    minutes = s//60
    seconds = s%60
    return f"{minutes}:{seconds}"

def convert_list_to_tuple(l):
    if len(l) == 1:
        return tuple(l[0])
    else:
        return tuple(l)

def get_overall(client_id, time_from, time_to, referrer, device_type, country, city):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()
    cur.execute(
        f"SELECT "
            f"SUM(sessions), "
            f"SUM(pageviews), "
            f"SUM(pageviews_w_timespent), "
            f"SUM(tot_time), "
            f"SUM(first_sessions) "
        f"FROM overall "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' "
            f"{referrer_clause}"
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
        f";"
    )
    records = cur.fetchone()
    cur.close()
    conn.close()
    sessions, pvs, pvs_w_time, tot_time, first_sessions = records
    if pvs_w_time is not None and pvs_w_time > 0:
        secs_per_pv = tot_time/pvs_w_time
        pvs_per_session = pvs/sessions
        mins_per_visit = convert_seconds_to_minutes(int(round(secs_per_pv * pvs_per_session)))
    else:
        mins_per_visit = "-"
    
    if sessions is not None and sessions > 0:
        first_visits = int(100*first_sessions/sessions)
        sessions = int(sessions)
    else:
        first_visits = "-"
        sessions = 0
    
    resp = {
        "sessions": sessions,
        "mins_per_visit": mins_per_visit,
        "first_visits": first_visits
    }
    return resp

def get_overall_url(client_id, time_from, time_to, url, referrer, device_type, country, city):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if url:
        url_clause = f" AND url_path = '{url}' "
    else:
        url_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()
    cur.execute(
        f"SELECT "
            f"SUM(pageviews), "
            f"SUM(pageviews_w_timespent), "
            f"SUM(tot_time), "
            f"SUM(sessions), "
            f"SUM(first_session_pageviews) "
        f"FROM url "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' AND "
            f"url_path = '{url}' "
            f"{referrer_clause}"
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
        f";"
    )
    records = cur.fetchone()
    cur.close()
    conn.close()

    pvs, pvs_w_time, tot_time, sessions, first_sessions = records
    if pvs_w_time is not None and pvs_w_time > 0:
        secs_per_pv = tot_time/pvs_w_time
        pvs_per_session = pvs/sessions
        mins_per_visit = convert_seconds_to_minutes(int(round(secs_per_pv * pvs_per_session)))
    else:
        mins_per_visit = "-"
    
    if sessions is not None and sessions > 0:
        first_visits = int(100*first_sessions/sessions)
        pvs = int(pvs)
    else:
        first_visits = "-"
        pvs = "-"
    resp = {
        "pvs": pvs,
        "mins_per_pv": mins_per_visit,
        "first_visits": first_visits
    }
    return resp

def get_trends(client_id, time_from, time_to, referrer, device_type, country, city, url):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if url:
        url_clause = f" AND url_path = '{url}' "
    else:
        url_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    if referrer or url:
        conn = make_connection()
        cur = conn.cursor()

        cur.execute(
            f"SELECT "
                f"hour, "
                f"SUM(pageviews) "
            f"FROM url "
            f"WHERE "
                f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
                f"client_id = '{client_id}' "
                f"{url_clause}"
                f"{referrer_clause}"
                f"{device_clause}"
                f"{country_clause}"
                f"{city_clause}"
            f"GROUP BY hour "
            f"ORDER BY hour "
            f";"
        )
        records = cur.fetchall()
        cur.close()
        conn.close()
        resp = {
            "total_pvs": []
        }
        for record in records:
            hour, pvs = record
            hour = 1000*int(datetime.strptime(hour, "%Y-%m-%d-%H").strftime("%s"))
            resp['total_pvs'].append([hour, int(pvs)])
    else:
        conn = make_connection()
        cur = conn.cursor()
        cur.execute(
            f"SELECT "
                f"hour, "
                f"SUM(pageviews), "
                f"SUM(search_pageviews), "
                f"SUM(social_pageviews), "
                f"SUM(forum_pageviews), "
                f"SUM(direct_pageviews), "
                f"SUM(other_pageviews) "
            f"FROM overall "
            f"WHERE "
                f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
                f"client_id = '{client_id}' "
                f"{device_clause}"
                f"{country_clause}"
                f"{city_clause}"
            f"GROUP BY hour "
            f"ORDER BY hour "
            f";"
        )
        records = cur.fetchall()
        cur.close()
        conn.close()
        resp = {
            "total_pvs": [],
            "search_pvs": [],
            "social_pvs": [],
            "direct_pvs": [],
            "forum_pvs": [],
            "other_pvs": []
        }
        for record in records:
            hour, pvs, search_pvs, social_pvs, forum_pvs, direct_pvs, other_pvs = record
            hour = 1000*int(datetime.strptime(hour, "%Y-%m-%d-%H").strftime("%s"))
            resp['total_pvs'].append([hour, int(pvs)])
            resp['search_pvs'].append([hour, int(search_pvs)])
            resp['social_pvs'].append([hour, int(social_pvs)])
            resp['direct_pvs'].append([hour, int(direct_pvs)])
            resp['forum_pvs'].append([hour, int(forum_pvs)])
            resp['other_pvs'].append([hour, int(other_pvs)])
    return resp

def get_trends_7d_ago(client_id, time_from, time_to, referrer, device_type, country, city):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()
    cur.execute(
        f"SELECT "
            f"hour, "
            f"SUM(pageviews) "
        f"FROM overall "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' "
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
            f"{referrer_clause}"
        f"GROUP BY hour "
        f"ORDER BY hour "
        f";"
    )
    records = cur.fetchall()
    cur.close()
    conn.close()
    resp = {
        "total_pvs": []
    }
    for record in records:
        hour, pvs = record
        hour = 1000*int(datetime.strptime(hour, "%Y-%m-%d-%H").strftime("%s"))
        resp['total_pvs'].append([hour, int(pvs)])
    return resp

def get_urls(client_id, time_from, time_to, referrer, device_type, country, city):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()
    cur.execute(
        f"SELECT "
            f"url_path, "
            f"SUM(pageviews) pageviews, "
            f"SUM(pageviews_w_timespent), "
            f"SUM(tot_time), "
            f"SUM(affluence_index), "
            f"SUM(first_session_pageviews) "
        f"FROM url "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' "
            f"{referrer_clause}"
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
        f"GROUP BY url_path "
        f"ORDER BY pageviews DESC "
        f";"
    )
    records = cur.fetchall()
    cur.close()
    conn.close()
    resp = {"data": []}
    for record in records:
        url, pvs, pvs_w_timespent, timespent, aff_idx, first_session_pvs = record
        if pvs_w_timespent is not None and pvs_w_timespent > 0:
            secs = int(round(timespent/pvs_w_timespent))
        else:
            secs = None
        resp['data'].append({
            'key': url,
            'url': url,
            'pvs': int(pvs),
            'secs': secs,
            'affidx': None,
            'newusers': int(100*round(first_session_pvs/pvs))
        })
    return resp

def get_macro(client_id, time_from, time_to, referrer, device_type, country, city, url, groupby="session_referrer", metric="secs_per_visit"):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    if url:
        url_clause = f" AND url_path = '{url}' "
    else:
        url_clause = ""

    conn = make_connection()
    cur = conn.cursor()

    if metric == "pvs_per_session":
        resp = []
    elif metric == "secs_per_visit":
        cur.execute(
            f"SELECT "
                f"{groupby}, "
                f"SUM(sessions) sessions, "
                f"SUM(pageviews), "
                f"SUM(pageviews_w_timespent), "
                f"SUM(tot_time) "
            f"FROM geography "
            f"WHERE "
                f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
                f"client_id = '{client_id}' "
                f"{url_clause}"
                f"{referrer_clause}"
                f"{device_clause}"
                f"{country_clause}"
                f"{city_clause}"
            f"GROUP BY {groupby} "
            f"ORDER BY sessions DESC"
            f";"
        )
        records = cur.fetchall()
        resp = {"data": []}
        for record in records:
            key, sessions, pvs, pvs_w_time, timespent = record
            if pvs_w_time is not None and pvs_w_time > 0:
                time_per_pv = timespent/pvs_w_time
                pvs_per_session = sessions/pvs
                time_per_session = time_per_pv*pvs_per_session
                time_per_session = int(round(time_per_session, 0))
            else:
                time_per_session = None
            resp['data'].append({
                "name": key,
                "value": int(sessions),
                "colorValue": time_per_session
            })
    elif metric == "aff_idx_per_visit":
        resp = []
    elif metric == "perc_new_users_visits":
        resp = []
    elif metric == "perc_mobile_visits":
        resp = []

    cur.close()
    conn.close()
    
    return resp

def get_geo(client_id, time_from, time_to, referrer, device_type, country, city, url):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    if url:
        url_clause = f" AND url_path = '{url}' "
    else:
        url_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()

    cur.execute(
        f"SELECT "
            f"country, "
            f"city, "
            f"SUM(sessions) sessions "
        f"FROM geography "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' "
            f"{url_clause}"
            f"{referrer_clause}"
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
        f"GROUP BY country, city "
        f"ORDER BY sessions DESC"
        f";"
    )
    records = cur.fetchall()
    cur.close()
    conn.close()
    resp = {"data": []}
    idx = 0
    for record in records:
        country, city, visits = record
        resp['data'].append({
            "key": str(idx), "country": country, "city": city, "visits": int(visits)
        })
        idx += 1

    return resp

def get_timespent(client_id, time_from, time_to, referrer, device_type, country, city, url):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    if url:
        url_clause = f" AND url_path = '{url}' "
    else:
        url_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()

    cur.execute(
        f"SELECT "
            f"SUM(pageviews_ts_lt_30), "
            f"SUM(pageviews_ts_30_60), "
            f"SUM(pageviews_ts_60_90), "
            f"SUM(pageviews_ts_90_120), "
            f"SUM(pageviews_ts_120_150), "
            f"SUM(pageviews_ts_150_180), "
            f"SUM(pageviews_ts_gt_180) "
        f"FROM url "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' "
            f"{url_clause}"
            f"{referrer_clause}"
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
        f";"
    )
    records = cur.fetchone()
    cur.close()
    conn.close()
    
    resp = {"data": []}
    for rec in records:
        resp['data'].append(int(rec))
    
    resp['data'] = [int(100*i/sum(resp['data'])) for i in resp['data']]
    
    return resp

def get_scroll_depth(client_id, time_from, time_to, referrer, device_type, country, city, url):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    if url:
        url_clause = f" AND url_path = '{url}' "
    else:
        url_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()

    cur.execute(
        f"SELECT "
            f"SUM(pageviews_sd_lt_20), "
            f"SUM(pageviews_sd_20_40), "
            f"SUM(pageviews_sd_40_60), "
            f"SUM(pageviews_sd_60_80), "
            f"SUM(pageviews_sd_gt_80) "
        f"FROM url "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' "
            f"{url_clause}"
            f"{referrer_clause}"
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
        f";"
    )
    records = cur.fetchone()
    cur.close()
    conn.close()
    
    resp = {"data": []}
    for rec in records:
        resp['data'].append(int(rec))
    
    resp['data'] = [int(100*i/sum(resp['data'])) for i in resp['data']]
    return resp

def get_read_next(client_id, time_from, time_to, referrer, device_type, country, city, url):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()

    cur.execute(
        f"SELECT "
            f"to_url, "
            f"SUM(pageviews) pageviews "
        f"FROM next_url "
        f"WHERE "
            f"hour BETWEEN '{time_from}' AND '{time_to}' AND "
            f"client_id = '{client_id}' AND "
            f"from_url = '{url}' "
            f"{referrer_clause}"
            f"{device_clause}"
            f"{country_clause}"
            f"{city_clause}"
        f"GROUP BY to_url "
        f"ORDER BY pageviews DESC"
        f";"
    )
    records = cur.fetchall()
    cur.close()
    conn.close()
    
    resp = {"data": []}
    for record in records:
        url, pvs = record
        resp['data'].append({"url": url, "pvs": int(pvs)})
    return resp


def get_events(client_id, time_from, time_to, referrer, device_type, country, city, url):
    if device_type:
        device_clause = f" AND device_type IN {device_type} "
    else:
        device_clause = ""
    
    if country:
        country_clause = f" AND country IN {country} "
    else:
        country_clause = ""
    
    if city:
        city_clause = f" AND city IN {city} "
    else:
        city_clause = ""
    
    if referrer:
        referrer_clause = f" AND session_referrer IN {referrer} "
    else:
        referrer_clause = ""
    
    conn = make_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM (
            SELECT
                'event1' event_cat,
                event,
                SUM(hits) hits
            FROM event1
            WHERE
                hour BETWEEN '{time_from}' AND '{time_to}' AND
                client_id = '{client_id}' AND 
                url_path = '{url}'
                {referrer_clause}
                {device_clause}
                {country_clause}
                {city_clause}
            GROUP BY event

            UNION
            
            SELECT
                'event2' event_cat,
                event,
                SUM(hits) hits
            FROM event2
            WHERE
                hour BETWEEN '{time_from}' AND '{time_to}' AND
                client_id = '{client_id}' AND 
                url_path = '{url}'
                {referrer_clause}
                {device_clause}
                {country_clause}
                {city_clause}
            GROUP BY event

            UNION

            SELECT
                'event3' event_cat,
                event,
                SUM(hits) hits
            FROM event3
            WHERE
                hour BETWEEN '{time_from}' AND '{time_to}' AND
                client_id = '{client_id}' AND 
                url_path = '{url}'
                {referrer_clause}
                {device_clause}
                {country_clause}
                {city_clause}
            GROUP BY event
        ) comb_table
        ORDER BY hits DESC
        ;
    """.format(time_from=time_from, time_to=time_to, client_id=client_id, url=url, referrer_clause=referrer_clause,
    device_clause=device_clause, country_clause=country_clause, city_clause=city_clause))

    records = cur.fetchall()
    cur.close()
    conn.close()
    
    resp = {"data": []}
    for record in records:
        event_cat, event, hits = record
        resp['data'].append({"event_cat": event_cat, "event": event, "hits": int(hits)})
    return resp

def serve_api(request):
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    api_params = request.get_json()
    api_endpoint = api_params.get("endpoint")
    
    client_id = api_params.get("client_id")
    time_from = api_params.get("time_from")
    time_to = api_params.get("time_to")
    
    referrer = api_params.get("referrer")
    if referrer:
        referrer = convert_list_to_tuple(referrer)
    
    device_type = api_params.get("device_type")
    if device_type:
        device_type = convert_list_to_tuple(device_type)
    
    country = api_params.get("country")
    if country:
        country = convert_list_to_tuple(country)
    
    city = api_params.get("city")
    if city:
        city = convert_list_to_tuple(city)

    url = api_params.get("url")
    groupby = api_params.get("groupby")
    metric = api_params.get("metric")

    if api_endpoint == "overall":
        #handle overall endpoint
        resp = get_overall(client_id, time_from, time_to, referrer, device_type, country, city)
    elif api_endpoint == "overall_url":
        #handle overall endpoint
        resp = get_overall_url(client_id, time_from, time_to, url, referrer, device_type, country, city)
    elif api_endpoint == "trends":
        #handle endpoint
        resp = get_trends(client_id, time_from, time_to,
            referrer, device_type, country, city, url)
    elif api_endpoint == "trends_7d_ago":
        #handle endpoint
        resp = get_trends_7d_ago(client_id, time_from, time_to, referrer, device_type, country, city)
    elif api_endpoint == "urls":
        #handle endpoint
        resp = get_urls(client_id, time_from, time_to,
            referrer, device_type, country, city)
    elif api_endpoint == "macro_agg":
        #handle endpoint
        resp = get_macro(client_id, time_from, time_to,
            referrer, device_type, country, city, url, groupby, metric)
    elif api_endpoint == "geo":
        #handle endpoint
        resp = get_geo(client_id, time_from, time_to,
            referrer, device_type, country, city, url)
    elif api_endpoint == "scroll_depth":
        #handle endpoint
        resp = get_scroll_depth(client_id, time_from, time_to,
            referrer, device_type, country, city, url)
    elif api_endpoint == "timespent":
        #handle endpoint
        resp = get_timespent(client_id, time_from, time_to,
            referrer, device_type, country, city, url)
    elif api_endpoint == "read_next":
        #handle endpoint
        resp = get_read_next(client_id, time_from, time_to,
            referrer, device_type, country, city, url)
    elif api_endpoint == "event_list":
        #handle endpoint
        resp = get_events(client_id, time_from, time_to,
            referrer, device_type, country, city, url)
    
    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }
    return (resp, 200, headers)

from flask import Flask, jsonify, request

import redis
from flask_httpauth import HTTPBasicAuth


def create_app(conn, auth_server_host, auth_server_port, test_config=None):
    app = Flask(__name__)
    auth = HTTPBasicAuth()
    conn = conn
    auth_server_host = auth_server_host
    auth_server_port = auth_server_port

    @auth.verify_password
    def verify_password(username, password):
        result = False
        try:
            auth_server_conn = redis.Redis(
                host=auth_server_host,
                port=auth_server_port,
                decode_responses=True,
                username=username,
                password=password,
            )
            auth_server_conn.ping()
            result = True
        except redis.exceptions.ResponseError:
            result = False
        except redis.exceptions.AuthenticationError:
            result = False
        return result

    @app.route("/", methods=["GET"])
    @auth.login_required
    def base():
        return jsonify({}), 200

    @app.route("/search", methods=["POST"])
    @auth.login_required
    def search():
        print(request.headers, request.get_json())
        reply = []
        return jsonify(reply), 200

    @app.route("/query", methods=["POST"])
    @auth.login_required
    def query():
        print(request.headers, request.get_json())
        input_json = request.json
        targets = input_json["targets"]
        reply = []
        for target in targets:
            redis_key_prefix = target["target"]
            if redis_key_prefix.startswith("text"):
                reply = from_redis_lists_to_grafana_table(redis_key_prefix, reply)
            if redis_key_prefix.startswith("flamegraph"):
                reply = from_redis_sorted_sets_to_grafana_flamegraph(
                    redis_key_prefix, reply
                )

        return jsonify(reply), 200

    def from_redis_lists_to_grafana_table(redis_key_prefix, reply):
        redis_columns_text = "{}:columns:text".format(redis_key_prefix)
        redis_columns_type = "{}:columns:type".format(redis_key_prefix)
        table_columns = conn.lrange(redis_columns_text, 0, -1)
        table_types = conn.lrange(redis_columns_type, 0, -1)
        if len(table_columns) == len(table_types):
            columns = []
            for pos, column_text in enumerate(table_columns):
                columns.append({"text": column_text, "type": table_types[pos]})
            rows = []
            cs = []
            for col in table_columns:
                redis_row_text = "{}:rows:{}".format(redis_key_prefix, col)
                cs.append(conn.lrange(redis_row_text, 0, -1))
            for x in range(0, len(cs[0])):
                rows.append([None for y in range(0, len(cs))])

            for col_n, col in enumerate(cs):
                for row_n, value in enumerate(col):
                    rows[row_n][col_n] = value
            reply.append(
                {
                    "columns": columns,
                    "rows": rows,
                    "type": "table",
                }
            )
        return reply

    return app


def from_redis_sorted_sets_to_grafana_flamegraph(redis_key_prefix, reply):
    v = {}
    reply = [
        {
            "columns": [{"text": "flamegraph", "type": "text"}],
            "rows": ['"{}"'.format(v)],
            "type": "table",
        }
    ]
    return reply

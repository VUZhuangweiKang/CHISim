import databus as dbs
from flask import Flask
import json
from influxdb import DataFrameClient
import argparse
import threading
import pickle
import pandas as pd
from flask import request

app = Flask(__name__)


def assign(node_type, node_cnt):
    query_str = 'SELECT * FROM "resource_pool" WHERE "node_type" = "%s" ORDER BY "timestamp" DESC LIMIT 1' % node_type
    results = db_client.query(query_str)
    if results:
        if results['available'] < node_cnt:
            return False, node_cnt - results['available']
        else:
            results['available'] -= node_cnt
            db_client.write_points(results, 'resource_pool')
            return True, 0
    else:
        return False, 0


def release(node_type, node_cnt):
    query_str = 'SELECT * FROM "resource_pool" WHERE "node_type" = "%s" ORDER BY "timestamp" DESC LIMIT 1' % node_type
    results = db_client.query(query_str)
    if results:
        results['available'] += node_cnt
        return True
    else:
        return False


@app.route('/get_free_nodes', method='GET')
def get_free_nodes():
    node_type = request.args.get('node_type')
    query_str = 'SELECT LAST("available") FROM "resource_pool" WHERE "node_type" = "%s"' % node_type
    results = db_client.query(query_str)
    if results:
        return 200, results['available']
    else:
        return 403, 'failed to query database'


# TODO: in-advance, on_demand_predict, on_demand_makeup均通过次acquire函数请求资源
@app.route('/acquire_nodes', method='POST')
def acquire_nodes():
    request_data = request.get_json()
    results = assign(request_data['node_type'], request_data['node_count'])
    if results[0]:
        return 200, 'OK'
    else:
        if results[1] == 0:
            return 403, 'failed to acquire nodes'
        else:
            return 201, str(results[1])


@app.route('/release_nodes', method='POST')
def release_nodes():
    request_data = request.get_json()
    results = release(request_data['node_type'], request_data['node_count'])
    if results:
        return 200, 'OK'
    else:
        return 403, 'failed to release nodes'


def process_machine_event(ch, method, properties, body):
    machine_event = pickle.loads(body)
    node_type = machine_event['node_type']
    query_str = 'SELECT * FROM "resource_pool" WHERE "node_type" = "%s" ORDER BY "timestamp" DESC LIMIT 1' % node_type
    results = db_client.query(query_str)

    if machine_event['EVENT'] in ['ENABLE', 'UPDATE']:
        if results:
            results['available'] += 1
            db_client.write_points(results, 'resource_pool')
        else:
            df = [{
                'timestamp': machine_event['EVENT_TIME'],
                'available': 1,
                'node_type': machine_event['node_type']
            }]
            df = pd.DataFrame(df)
            db_client.write_points(df, 'resource_pool')
    elif machine_event['EVENT'] in ['DISABLE']:
        if results:
            results['available'] -= 1
            db_client.write_points(results, 'resource_pool')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, help='host IP for running the resource manager')
    args = parser.parse_args()

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = DataFrameClient(*db_info)
    app.run()

    dbs_connection = dbs.init_connection()
    resource_manager_channel = dbs_connection.channel()
    # listen machine events
    lme = threading.Thread(name='listen_machine_events', target=dbs.consume, args=('resource_manager', 'machine_events', 'raw_event', process_machine_event, dbs_connection))

    lme.start()
    app.run(host=args.host, port=5000, debug=False)
    lme.join()
    dbs_connection.close()

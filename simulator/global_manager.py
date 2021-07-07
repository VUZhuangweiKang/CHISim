import databus as dbs
import json
import influxdb


if __name__ == '__main__':
    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = influxdb.InfluxDBClient(host=db_info['host'], username=db_info['username'], password=db_info['password'])
    db_list = [db['name'] for db in db_client.get_list_database()]
    if 'UserRequests' not in db_list:
        db_client.create_database('UserRequests')

    dbs_connection = dbs.init_connection()
    gm_channel = dbs_connection.channel()
    dbs.clear_databus(gm_channel)

    gm_channel.exchange_declare('internal_exchange')
    gm_channel.exchange_declare('user_requests_exchange')
    gm_channel.exchange_declare('machine_events_exchange')
    gm_channel.exchange_declare('osg_jobs_exchange')
    gm_channel.queue_declare(queue='user_requests_queue')
    gm_channel.queue_declare(queue='machine_events_queue')
    gm_channel.queue_declare(queue='osg_jobs_queue')
    gm_channel.queue_declare(queue='internal_queue')
    
    while True:
        pass


import databus as dbs
import requests
import threading


if __name__ == '__main__':
    dbs_connection = dbs.init_connection()

    listen_frontend = threading.Thread(name='resource_scheduler',
                                            target=dbs.consume,
                                            args=('frontend', 'user_requests', 'raw_request', dump_usr_request,
                                                  dbs_connection))
    listen_forecaster = threading.Thread(name='resource_scheduler',
                                            target=dbs.consume,
                                            args=('frontend', 'user_requests', 'raw_request', dump_usr_request,
                                                  dbs_connection))

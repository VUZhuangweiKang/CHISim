import argparse
from AsyncDatabus.Publisher import Publisher


if __name__ == '__main__':
    parser = argparse.ArgumentParser()    
    parser.add_argument('--datafile', type=str, help='The path of the payload file', default='../datasets/lease_info/compute_haswell.csv')
    parser.add_argument('--index_col', type=int, help='The index of the column being used as the index of the payload file', default=5)
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=100000)
    args = parser.parse_args()
    args = vars(args)
    args.update({'exchange': 'user_requests_exchange', 'routing_key': 'raw_request'})

    pub = Publisher('amqp://chi-sim:chi-sim@localhost:5672/%2F?connection_attempts=3&heartbeat=0')
    pub.run()
    pub.emit_timeseries(**args)


import argparse
import databus as dbs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--payload', type=str, help='The path of the payload file', default='../datasets/machine_events/compute_haswell.csv')
    parser.add_argument('--index_col', type=int, help='The index of the column being used as the index of the payload file', default=0)
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=10000)
    args = parser.parse_args()
    args = vars(args)
    args.update({'exchange': 'machine_events_exchange', 'routing_key': 'machine_event'})
    dbs.emit_timeseries(**args)


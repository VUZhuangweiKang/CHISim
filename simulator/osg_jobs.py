import argparse
import databus as dbs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--payload', type=str, help='The path of the payload file', default='../datasets/osg_jobs/osg_jobs.csv')
    parser.add_argument('--index_col', type=int, help='The index of the column being used as the index of the payload file', default=13)
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=10000)
    parser.add_argument('--no_print', action='store_true', help='print publication details', default=False)
    args = parser.parse_args()
    args = vars(args)
    args.update({'exchange': 'osg_jobs_exchange', 'routing_key': 'osg_job'})
    dbs.emit_timeseries(**args)
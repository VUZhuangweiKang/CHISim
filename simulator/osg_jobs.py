import argparse
from workload import WorkloadStream


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--datafile', type=str, help='The path of the payload file', default='../datasets/osg_jobs/osg_jobs.csv')
    parser.add_argument('--index_col', type=int, help='The index of the column being used as the index of the payload file', default=13)
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=100000)
    args = parser.parse_args()
    args = vars(args)
    args.update({'exchange': 'osg_jobs_exchange', 'queue': 'osg_jobs_queue', 'routing_key': 'osg_job'})

    osg_workload = WorkloadStream(amqpurl='amqp://chi-sim:chi-sim@localhost:5672/%2F?connection_attempts=3&heartbeat=0', **args)
    osg_workload.run()
import argparse
import databus as dbs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    amqp_args = parser.add_argument_group('RabbitMQ Args')
    amqp_args.add_argument('exchange', type=str, default='default')
    amqp_args.add_argument('routing_key', type=str, required=True)

    ch_osg_args = parser.add_argument_group('Chameleon-OSG Args')
    ch_osg_args.add_argument('payload', type=str, help='The path of the payload file', required=True)
    ch_osg_args.add_argument('index_col', type=int, help='The index of the column being used as the index of the payload file', default=0)
    ch_osg_args.add_argument('scale_ratio', type=float, help='The ratio for scaling down the time series data', default=1)
    ch_osg_args.add_argument('print', action='store_true', help='print publication details', default=False)
    args = parser.parse_args()

    dbs.emit_timeseries(args.exchange, args.routing_key, args.payload, args.index_col, args.scale_ratio)


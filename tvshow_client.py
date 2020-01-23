
from __future__ import print_function
import logging

import grpc

import tvshow_pb2
import tvshow_pb2_grpc


def run():

    with grpc.insecure_channel('localhost:50051') as channel:
        stub = tvshow_pb2_grpc.TvshowRPCStub(channel)

        iterator = stub.getGoodTvShows(tvshow_pb2.ShowRequest(min_rating='8.1'))
        for tvshow in iterator:
            print(tvshow)

try:
    run()
except KeyboardInterrupt:
    pass

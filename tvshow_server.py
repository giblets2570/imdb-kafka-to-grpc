from concurrent import futures
import logging

import tvshow_pb2
import tvshow_pb2_grpc
import time
import json
import grpc
from kafka import KafkaConsumer, KafkaProducer

class TvshowRPCServicer(tvshow_pb2_grpc.TvshowRPCServicer):
    """docstring for TvShow."""

    def getGoodTvShows(self, request, context):
        consumer = KafkaConsumer(
            'parsed_pages',
            auto_offset_reset='earliest',
            bootstrap_servers=['localhost:9092'],
            consumer_timeout_ms=1000
        )
        show_cache = {}
        try:
            while True:
                topic_partitions = consumer.poll(500)
                if len(topic_partitions.keys()) == 0:
                    continue
                for partition in topic_partitions.values():
                    for msg in partition:
                        value = json.loads(msg.value)
                        if (value['title'] in show_cache):
                            continue
                        show_cache[value['title']] = value['rating']
                        if (float(value['rating']) < float(request.min_rating)):
                            continue
                        yield tvshow_pb2.ShowResponse(
                            name=value['title'],
                            rating=value['rating'],
                        )
        except Exception as e:
            print(e)
        finally:
            consumer.close()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    tvshow_pb2_grpc.add_TvshowRPCServicer_to_server(TvshowRPCServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    serve()

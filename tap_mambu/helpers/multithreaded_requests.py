from concurrent.futures import Future
from threading import Thread


class MultithreadedRequestsPool:
    pool_size = 100
    dispatcher_thread = None

    @classmethod
    def run(cls):
        pass

    @classmethod
    def start(cls):
        if cls.dispatcher_thread is None:
            cls.dispatcher_thread = Thread(target=cls.run, daemon=True, name="MultithreadedRequestPoolDispatcher")
            cls.dispatcher_thread.start()

    @classmethod
    def queue_request(cls, client, stream_name,
                      endpoint_path, endpoint_api_method, endpoint_api_version,
                      endpoint_body, endpoint_params) -> Future:
        pass

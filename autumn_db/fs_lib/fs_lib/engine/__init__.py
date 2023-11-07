import os
from multiprocessing.pool import ThreadPool


class Dispatcher:

    def __init__(self):
        self._under_processing = set()

        processors = os.cpu_count()
        self._pool = ThreadPool(processes=processors)

    def writes_to(self):
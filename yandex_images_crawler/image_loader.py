import hashlib
import io
import logging
from multiprocessing import Queue, Value, get_logger
from pathlib import Path
from typing import FrozenSet, Tuple, Union

import numpy as np
import requests
from PIL import Image
from imgdl import download

requests.packages.urllib3.disable_warnings()


class ImageLoader:
    def __init__(
        self,
        load_queue: Queue,
        image_size: Tuple[int, int],
        image_dir: Union[str, Path],
        skip_files: FrozenSet[str] = frozenset(),
        is_active: Value = Value("i", True),
    ):
        self.load_queue = load_queue
        self.min_width, self.min_height = image_size
        self.image_dir = Path(image_dir)
        self.skip_files = skip_files
        self.is_active = is_active

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
            "Referer": "https://yandex.com/",
        }

        self.logger = get_logger()
        self.logger.addHandler(logging.StreamHandler())
        self.logger.setLevel(logging.INFO)

    def run(self):
        while True:
            if not self.is_active.value:
                return

            link, (width, height) = self.load_queue.get()
            download([link], store_path=self.image_dir)
            
import hashlib
import io
import logging
from multiprocessing import Queue, Value, get_logger
from pathlib import Path
from typing import FrozenSet, Tuple, Union

import numpy as np
import requests
from PIL import Image
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from .imgdl import download

requests.packages.urllib3.disable_warnings()


class ImageLoader:

    def __init__(self,
                 images_count: int,
                 load_queue: Queue,
                 image_dir: Union[str, Path],
                 is_active,
                 chunk_size: int = 1,
                 ) -> None:
        self.images_count = images_count
        self.load_queue = load_queue
        self.image_dir = Path(image_dir)
        self.is_active = is_active
        self.chunk_size = chunk_size

        self.total_downloaded_count = 0

        self.logger = get_logger()
        self.logger.addHandler(logging.StreamHandler())
        self.logger.setLevel(logging.INFO)

        self.progress_bar = tqdm(total=images_count)

    def __log(self, msg: str):
        with logging_redirect_tqdm():
            self.logger.info(msg)

    def __download_images(self, count: int) -> None:
        images = []
        for _ in range(count):
            image = self.load_queue.get()
            images.append(image.link)

        paths = download(images, store_path=self.image_dir, verbose=False)
        downloaded_count = len([path for path in paths if path is not None])
        
        if downloaded_count != count:
            self.__log(f"Failed to load {count} images; loaded count: {downloaded_count}")
        
        self.progress_bar.update(downloaded_count)
        self.total_downloaded_count += downloaded_count

    def run(self) -> None:
        while True:
            if not self.is_active.value:
                if self.load_queue.qsize() > 0:
                    self.__download_images(self.load_queue.qsize())
                    self.__log(f"Downloaded {self.total_downloaded_count} image{'s' if self.images_count > 1 else ''}")
                self.progress_bar.close()
                return

            if self.load_queue.qsize() >= self.chunk_size:
                self.__download_images(self.chunk_size)
            
            if self.total_downloaded_count == self.images_count:
                self.__log(f"Downloaded all {self.images_count} image{'s' if self.images_count > 1 else ''}")
                self.is_active.value = False
                self.progress_bar.close()
                return

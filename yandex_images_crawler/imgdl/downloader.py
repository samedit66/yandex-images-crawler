import random
from collections.abc import Iterable
from concurrent import futures
from dataclasses import dataclass
from io import BytesIO
from time import sleep
from typing import Iterable
from pathlib import Path

import requests
from PIL import Image

from .settings import config
from .storage.backend import BaseStorage, resolve_storage_backend


@dataclass(frozen=True)
class DownloadingResult:
    downloaded: list[Path]
    '''List of paths of downloaded images'''
    failed: list[str]
    '''List of urls of failed to download images'''


class ImageDownloader(object):
    """Image downloader that converts to common format.

    Downloads images and converts them to JPG format and RGB mode.

    Parameters
    ----------
    storage : BaseStorage
        Storage backend
    n_workers : int
        Number of simultaneous threads to use
    timeout : float
        Timeout to be given to the url request
    min_wait : float
        Minimum wait time between image downloads
    max_wait : float
        Maximum wait time between image downloads
    session : requests.Session
        requests session
    """

    def __init__(self,
                 storage: BaseStorage | None = None,
                 n_workers: int = config.N_WORKERS,
                 timeout: float = config.TIMEOUT,
                 min_wait: float = config.MIN_WAIT,
                 max_wait: float = config.MAX_WAIT,
                 session: requests.Session | None = None,
                 ) -> None:
        self.storage = resolve_storage_backend(config.STORE_PATH) if storage is None else storage
        self.n_workers = n_workers
        self.timeout = timeout
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.session = requests.Session() if session is None else session

    def __call__(self,
                 urls: str | Iterable[str],
                 paths: str | list | None = None,
                 force: bool = False,
                 ) -> DownloadingResult:
        """Download url or list of urls

        Parameters
        ----------
        urls : str | list
            url or list of urls to be downloaded

        path : str | list
            path or list of paths where the image(s) should be stored

        force : bool
            If True force the download even if the files already exists

        verbose: bool
            If True show the progress bar of downloading

        Returns
        -------
        downloading_result: DownloadingResult
            Object with information about downloaded images and failed to download ones
        """
        urls = list(urls)
        if paths is None:
            paths = [None] * len(urls)

        with futures.ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            future_to_url = {
                executor.submit(self._download_image, url, path, force): url
                for url, path in zip(urls, paths)
            }

            total = len(future_to_url)
            paths = [None] * total
            
            downloaded = []
            failed = []
            for future in futures.as_completed(future_to_url):
                url = future_to_url[future]
                if future.exception() is None:
                    path = Path(str(future.result()))
                    downloaded.append(path)
                else:
                    failed.append(url)

        return DownloadingResult(downloaded=downloaded,
                                 failed=failed,
                                 )

    def _download_image(self, url, path=None, force=False):
        """Download image and convert to jpeg rgb mode.

        If the image path already exists, it considers that the file has
        already been downloaded and does not downloaded again.


        Parameters
        ----------
        url : str
            url of the image to be downloaded

        path : str
            path where the image should be stored

        force : bool
            If True force the download even if the file already exists

        Returns
        -------
        path : str
            Path where the image was stored
        """
        metadata = {
            "success": False,
            "url": url,
            "session": {
                "headers": dict(self.session.headers),
                "timeout": self.timeout,
            },
        }

        path = path or self.storage.get_filepath(url)
        if self.storage.exists(path) and not force:
            metadata.update({"success": True, "filepath": path})
            return path
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            orig_img = Image.open(BytesIO(response.content))
            img = self.convert_image(orig_img)
            self.storage.save(img, path)

            metadata.update(
                {
                    "success": True,
                    "filepath": path,
                    "response": {
                        "headers": dict(response.headers),
                        "status_code": response.status_code,
                    },
                }
            )

            sleep(random.uniform(self.min_wait, self.max_wait))
        except Exception as e:
            metadata.update(
                {
                    "Exception": {
                        "type": type(e),
                        "msg": str(e),
                    },
                }
            )
            if "response" in locals():
                metadata.update(
                    {
                        "response": {
                            "headers": dict(response.headers),
                            "status_code": response.status_code,
                        },
                    }
                )

            raise e
        
        return path

    def get(self, url: str) -> Image:
        response = self.session.get(url, timeout=self.timeout)
        return Image.open(BytesIO(response.content))

    @staticmethod
    def convert_image(img: Image) -> Image:
        """Convert images to JPG, RGB mode and given size if any.

        Parameters
        ----------
        img : Pil.Image

        Returns
        -------
        img : Pil.Image
            Converted image in Pil format
        buf : BytesIO
            Buffer of the converted image
        """
        if img.format == "PNG" and img.mode == "RGBA":
            background = Image.new("RGBA", img.size, (255, 255, 255))
            background.paste(img, img)
            img = background.convert("RGB")
        elif img.mode == "P":
            img = img.convert("RGBA")
            background = Image.new("RGBA", img.size, (255, 255, 255))
            background.paste(img, img)
            img = background.convert("RGB")
        elif img.mode != "RGB":
            img = img.convert("RGB")

        return img

    @staticmethod
    def resize_image(img, size: tuple[int, int]) -> Image:
        """Resize an image to a given size."""
        img = img.copy()
        img.thumbnail(size, Image.ANTIALIAS)
        return img


def download(urls: list[str],
             paths: list | None = None,
             store_path: str | Path = config.STORE_PATH,
             n_workers: int = config.N_WORKERS,
             timeout: float = config.TIMEOUT,
             min_wait: float = config.MIN_WAIT,
             max_wait: float = config.MAX_WAIT,
             session: requests.Session = requests.Session(),
             force: bool = False,
             ) -> DownloadingResult:
    """Asynchronously download images using multiple threads.

    Parameters
    ----------
    urls : iterator
        Iterator of urls
    path : list
        list of paths where the images should be stored
    store_path : str
        Root path where images should be stored
    n_workers : int
        Number of simultaneous threads to use
    timeout : float
        Timeout to be given to the url request
    min_wait : float
        Minimum wait time between image downloads
    max_wait : float
        Maximum wait time between image downloads
    force : bool
        If True force the download even if the files already exists
        
    Returns
    -------
    downloading_result: DownloadingResult
        Object with information about downloaded images and failed to download ones
    """
    downloader = ImageDownloader(
        storage=resolve_storage_backend(store_path=store_path),
        n_workers=n_workers,
        timeout=timeout,
        min_wait=min_wait,
        max_wait=max_wait,
        session=session,
    )

    return downloader(urls, paths=paths, force=force)

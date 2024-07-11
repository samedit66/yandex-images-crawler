try:
    from .image_loader import ImageLoader
    from .yandex_crawler import YandexCrawler
except ImportError:
    from yandex_crawler import YandexCrawler
    from image_loader import ImageLoader

import argparse
import logging
import os
from multiprocessing import Process, Queue, Value
from pathlib import Path
from typing import List, Union


def __start_crawler(start_link: str, load_queue: Queue, id: int, is_active):
    crawler = YandexCrawler(
        start_link=start_link,
        load_queue=load_queue,
        id=id,
        is_active=is_active,
    )
    crawler.run()


def __start_loader(
    images_count: int,
    load_queue: Queue,
    image_dir: Union[str, Path],
    is_active,
):
    crawler = ImageLoader(
        images_count=images_count,
        load_queue=load_queue,
        image_dir=image_dir,
        is_active=is_active,
    )
    crawler.run()


def download(
    links: List[str],
    image_count: int,
    image_dir: Union[str, Path],
):
    proc_num = len(links)

    load_queue = Queue(10 * proc_num)

    is_active = Value("i", True)

    crawlers = [
        Process(
            target=__start_crawler,
            args=(links[i], load_queue, i, is_active),
            daemon=True,
        )
        for i in range(proc_num)
    ]

    loaders = [
        Process(
            target=__start_loader,
            args=(image_count, load_queue, image_dir, is_active),
            daemon=True,
        )
        for _ in range(proc_num * 2)
    ]

    processes = []
    processes.extend(crawlers)
    processes.extend(loaders)

    for process in processes:
        process.start()

    for process in processes:
        process.join()


def __parse_args():
    parser = argparse.ArgumentParser(description="Yandex Images Crawler", add_help=True)
    parser.add_argument(
        "--links",
        type=str,
        metavar="LINK1,...",
        default=None,
        required=False,
        help="""Full links to image sets for download.
        Links should be separated by commas.
        Each link should lead to an open preview of the image.""",
    )
    parser.add_argument(
        "--links-file",
        type=str,
        metavar="FILE",
        default=None,
        required=False,
        help="""Text file with full links to image sets for download.
        Links should be separated by newlines.
        Each link should lead to an open preview of the image.""",
    )
    parser.add_argument(
        "--size",
        type=str,
        metavar="WxH",
        default="0x0",
        required=False,
        help="""Minimum size of images to download.
        Width an height should be separated by 'x'.""",
    )
    parser.add_argument(
        "--count",
        type=int,
        metavar="N",
        default=0,
        required=False,
        help="""Required count of images to download.
        A message appears if the desired number of images are downloaded.""",
    )
    parser.add_argument(
        "--dir",
        type=str,
        metavar="DIR",
        default="yandex-images",
        required=False,
        help="Directory for new images.",
    )
    parser.add_argument(
        "--prev-dir",
        type=str,
        metavar="DIR",
        default=None,
        required=False,
        help="""Directory of previously loaded images.
        Program skips the loading of already loaded images in another directory.
        Useful for re-downloading.""",
    )

    args = parser.parse_args()
    new_args = argparse.Namespace()

    new_args.links = []
    if args.links is not None and args.links_file is not None:
        logging.critical("Provide links via --links only or --links_file only")
        exit(-1)
    if args.links is not None:
        new_args.links = args.links.split(",")
    if args.links_file is not None:
        with open(args.links_file, "r") as f:
            new_args.links = f.read().strip().split()
    if len(new_args.links) == 0:
        logging.critical("Provide links via --links only or --links_file only")
        exit(-1)

    new_args.size = tuple(map(int, args.size.split("x")))

    new_args.count = args.count

    new_args.image_dir = Path(args.dir)
    new_args.image_dir.mkdir(parents=True, exist_ok=True)

    previous_images = []
    for _, _, files in os.walk(new_args.image_dir):
        previous_images.extend(files)
        if new_args.count > 0:
            new_args.count += len(files)
        break

    if args.prev_dir is not None:
        for _, _, files in os.walk(args.prev_dir):
            previous_images.extend(files)

    new_args.skip_files = frozenset([file.split(".")[0] for file in previous_images])

    return new_args


def main():
    args = __parse_args()
    download(args.links, args.count, args.image_dir)


if __name__ == "__main__":
    main()

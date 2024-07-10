from collections import namedtuple
import time
from multiprocessing import Queue, Value

from selenium import webdriver
from selenium.webdriver.common.by import By


class ReachedEndError(Exception):
    ...


Image = namedtuple('Image', 'link width height')


class YandexCrawler:

    def __init__(self,
                 start_link: str,
                 load_queue: Queue,
                 links_counter: Value,
                 id: int = 0,
                 ) -> None:
        self.start_link = start_link
        self.load_queue = load_queue
        self.links_counter = links_counter
        self.id = str(id)
        self.driver = webdriver.Firefox()
        self.started_crawling = False

    def __get_image_link(self) -> None:
        width, height = None, None
        try:
            width, height = [
                int(i)
                for i in self.driver.find_element(
                    By.CSS_SELECTOR, "span[class*='OpenImageButton-SaveSize']"
                ).text.split("×")
            ]
        except:
            for elem in self.driver.find_elements(By.CSS_SELECTOR, "span[class*='Button2-Text']"):
                try:
                    width, height = [int(i) for i in elem.text.split("×")]
                    break
                except:
                    pass

        link = self.driver.find_element(By.CLASS_NAME, "MMImage-Preview").get_attribute("src")
        time.sleep(0.1)
        self.load_queue.put(Image(link=link, width=width, height=height)) 

    def __next_preview(self) -> None:
        try:
            btn = self.driver.find_element(By.CSS_SELECTOR, "button[class*='CircleButton_type_next']")
            btn.click()
        except:
            raise ReachedEndError

    def run(self) -> int:
        if not self.started_crawling:
            self.started_crawling = True
            self.driver.get(self.start_link)

        crawled_count = 0
        while True:
            try:
                self.__get_image_link()
                self.__next_preview()
            except ReachedEndError:
                self.driver.close()
                break

        return crawled_count
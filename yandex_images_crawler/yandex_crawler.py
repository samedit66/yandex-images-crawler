from collections import namedtuple
import time
from multiprocessing import Queue

from selenium import webdriver
from selenium.webdriver.common.by import By


class ReachedEndError(Exception):
    ...


Image = namedtuple('Image', 'link width height')


class YandexCrawler:

    def __init__(self,
                 start_link: str,
                 load_queue: Queue,
                 is_active,
                 #min_wait: float,
                 #max_wait: float,
                 id: int = 0,
                 ) -> None:
        self.start_link = start_link
        self.load_queue = load_queue
        self.is_active = is_active
        self.id = str(id)
        self.driver = webdriver.Firefox()

    def _get_image_link(self) -> None:
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

    def _next_preview(self) -> None:
        try:
            btn = self.driver.find_element(By.CSS_SELECTOR, "button[class*='CircleButton_type_next']")
            btn.click()
        except:
            raise ReachedEndError

    def run(self) -> None:
        self.driver.get(self.start_link)

        while True:
            if not self.is_active.value:
                self.driver.close()
                break

            try:
                self._get_image_link()
                self._next_preview()
            except ReachedEndError:
                break
            finally:
                self.driver.close()
    
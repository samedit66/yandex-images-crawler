import logging
import time
from multiprocessing import Queue, Value, get_logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


class YandexCrawler:
    def __init__(self,
                 links_count: int,
                 start_link: str,
                 load_queue: Queue,
                 id=0,
                 is_active: Value = Value("i", True),
                 ):
        self.links_count = links_count
        self.start_link = start_link
        self.load_queue = load_queue
        self.id = str(id)
        self.is_active = is_active

        self.driver = webdriver.Firefox()

        self.logger = get_logger()
        self.logger.addHandler(logging.StreamHandler())
        self.logger.setLevel(logging.INFO)

        self.prog_bar = tqdm(total=self.links_count)

    def __get_image_link(self):
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
        self.load_queue.put((link, (width, height)))
        self.prog_bar.update(1)

    def __next_preview(self):
        try:
            btn = self.driver.find_element(By.CSS_SELECTOR, "button[class*='CircleButton_type_next']")
            btn.click()
        except:
            with logging_redirect_tqdm():
                self.logger.critical(f"Process #{self.id} can't move to the next image.")

    def run(self):
        self.driver.get(self.start_link)
        while True:
            if not self.is_active.value:
                self.driver.close()
                return

            try:
                self.__get_image_link()
                self.__next_preview()
            except Exception as e:
                with logging_redirect_tqdm():
                    self.logger.critical(e)
                    time.sleep(10)

import time
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


class SeleniumWebDriverContextManager:
    def __init__(self, driver_path=None, options=None):
        self.driver_path = driver_path
        self.options = options
        self.driver = None

    def __enter__(self):
        service = Service(executable_path=self.driver_path)
        options = Options()
        self.driver = webdriver.Chrome(service=service, options=options)
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver:
            self.driver.quit()


if __name__ == "__main__":
    with SeleniumWebDriverContextManager(driver_path= r'C:\Users\vladyslav_yevtushenk\dqe_automation\dqe-automation\Selenium Introduction\chromedriver-win64\chromedriver.exe') as driver:
        #task1
        path = os.path.abspath("report.html")
        driver.get(path)
        driver.set_window_size(1920, 1080)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, "table")))
        data = {}
        table = driver.find_element(By.CLASS_NAME, "table")
        columns =  table.find_elements(By.CLASS_NAME, "y-column")
        for item in columns:
            by_xpath = item.find_element(By.XPATH, './/*[@id="header"]')
            row_element = item.find_element(By.ID, "cells1")
            rows = item.find_elements(By.CLASS_NAME, "column-cell")
            lst = [row.text for row in rows if row.text != by_xpath.text]
            data[by_xpath.text] = lst
        df = pd.DataFrame(data)
        df.to_csv(r'table.csv', index=False)

        #task2
        doughnut = driver.find_element(By.CLASS_NAME, "trace")
        doughnut.screenshot(f"screenshot0.png") #initial screen
        filters = driver.find_elements(By.CLASS_NAME, "traces")
        for i in range(len(filters)):
            active_filters = [el for el in filters if el.value_of_css_property("opacity") == "1"]  # click only active filter
            button = active_filters[0].find_element(By.CLASS_NAME, "legendtoggle")
            slices = doughnut.find_elements(By.CSS_SELECTOR, 'text.slicetext[data-notex="1"]')
            all_rows = []
            for slice in slices:
                values = slice.find_elements(By.TAG_NAME, "tspan")
                row = [value.text for value in values]
                all_rows.append(row)
            print(all_rows)
            df = pd.DataFrame(all_rows, columns=['Facility Type', 'Min Average Time Spent'])
            df.to_csv(f'doughnut{i+1}.csv', index=False)
            button.click()
            time.sleep(2)
            try:
                doughnut = driver.find_element(By.CLASS_NAME, "trace")
                display = doughnut.find_elements(By.CSS_SELECTOR, 'text.slicetext[data-notex="1"]')
                if not display:
                    raise NoSuchElementException
                doughnut.screenshot(f"screenshot{i+1}.png")
            except NoSuchElementException:
                 print("doughnut not displayed")
                 break



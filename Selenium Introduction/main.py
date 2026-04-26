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
        driver.get(r"C:\Users\vladyslav_yevtushenk\dqe_automation\dqe-automation\Selenium Introduction\report.html")
        driver.set_window_size(1920, 1080)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, "table")))
        #task parse table to csv
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
        df.to_csv(r'output.csv', index=False)
        filters = driver.find_element(By.CLASS_NAME, "groups")
        buttons = filters.find_elements(By.CLASS_NAME, "legendtoggle")
        for i in range(len(buttons)):
            filters = driver.find_element(By.CLASS_NAME, "groups")
            buttons = filters.find_elements(By.CLASS_NAME, "legendtoggle")
            button = buttons[i]
            button.click()
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, "trace")))
            doughnut = driver.find_element(By.CLASS_NAME, "trace")
            doughnut.screenshot(f"screenshot{i}.png")
            try:
                WebDriverWait(doughnut, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'text.slicetext[data-notex="1"]')))
                slices = doughnut.find_elements(By.CSS_SELECTOR, 'text.slicetext[data-notex="1"]')
                all_rows = []
                for slice in slices:
                    values = slice.find_elements(By.TAG_NAME, "tspan")
                    row = [value.text for value in values]
                    all_rows.append(row)
                print(all_rows)
                df = pd.DataFrame(all_rows, columns=['Facility Type', 'Min Average Time Spent'])
                df.to_csv(f'doughnut{i}.csv', index=False)
            except NoSuchElementException:
                print("Элементы slicetext не найдены после фильтра.")



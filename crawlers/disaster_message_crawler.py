from glob import glob
from csv import writer
from utils import print_time
from datetime import datetime
from selenium import webdriver
from os import rename, getcwd, chdir
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC


class Message_loaded:
    def __init__(self, locator):
        self.locator = locator

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        return element if '재난문자' in element.text else False


@print_time
def disaster_message_crawler():
    time = None
    t = '20111118074344'
    date = '20160609'
    start = '2011/11/18 07:43:44'
    path = './data/raw\\disaster_messages_'

    if getcwd()[-16:] != 'COVID-19 Project':
        chdir('..')

    file_list = glob(f'{path}*')

    if file_list:
        from re import compile

        p = compile(r'\d{14}')
        t = p.search(file_list[0]).group()
        new_date = datetime.strptime(t[:8], '%Y%m%d')
        start = f'{t[:4]}/{t[4:6]}/{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:]}'

        if new_date > datetime.strptime(date, '%Y%m%d'):
            date = datetime.strftime(new_date, '%Y%m%d')

        f = open(file_list[0], 'a', encoding='utf-8', newline='')
    else:
        f = open(f'{path}.csv', 'w', encoding='utf-8', newline='')

    wr = writer(f)

    with webdriver.Chrome() as driver:
        wait = WebDriverWait(driver, 5)

        if not file_list:
            wr.writerow(['time', 'body', 'to'])

        driver.get(
            'http://www.safekorea.go.kr/idsiSFK/neo/sfk/cs/sfc/dis/'
            'disasterMsgList.jsp?menuSeq=679'
            )

        inputs = driver.find_elements_by_xpath('//ul//input')

        inputs[1].clear()
        inputs[2].clear()
        inputs[0].send_keys(start)
        inputs[1].send_keys(date)
        inputs[2].send_keys(date)

        check_stale = driver.find_element_by_id('bbs_tr_0_bbs_title')
        driver.find_element_by_class_name('search_btn').click()
        wait.until(EC.staleness_of(check_stale))
        driver.find_element_by_id('bbs_tr_0_bbs_title').click()

        while True:
            try:
                next = wait.until(Message_loaded((By.ID, 'bbs_next')))
                next.click()
                title = wait.until(Message_loaded((By.ID, 'sj')))
            except TimeoutException:
                break

            time = title.text[:-5]
            text = driver.find_element_by_id('cn').text.split('-송출지역-')
            body = text[0].strip().replace('\n', ' ')
            if len(text) == 1:
                to = None
            else:
                to = text[1].strip().replace('\n', ', ')

            wr.writerow([time, body, to])

    f.close()

    if time is not None:
        t = time.translate({ord(x): '' for x in ['/', ':', ' ']})
        old_name = file_list[0] if file_list else f'{path}.csv'
        rename(old_name, f'{path}{t}.csv')


if __name__ == '__main__':
    disaster_message_crawler()

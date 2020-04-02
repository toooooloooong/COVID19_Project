from selenium import webdriver
from utils import print_time, File_manager
from selenium.webdriver.common.by import By
from multiprocessing import Process, Manager
from pandas import read_csv, DataFrame, concat
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC


class Message_loaded:
    def __init__(self, locator):
        self.locator = locator

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        return element if '재난문자' in element.text else False


def init_page(driver, wait):
    driver.get(
        'http://www.safekorea.go.kr/idsiSFK/neo/sfk/cs/sfc/dis/'
        'disasterMsgList.jsp?menuSeq=679'
        )
    check_stale = wait.until(
        EC.presence_of_element_located((By.ID, 'bbs_tr_0_bbs_title'))
        )
    search_end = driver.find_element_by_id('search_end')
    search_end.clear()
    search_end.send_keys('ㄱ')
    driver.find_element_by_class_name('search_btn').click()
    wait.until(EC.staleness_of(check_stale))


def get_split(total, n, num_workers):
    split = []

    if n < 1598:
        m = (total - 1598) / (num_workers - 1)
        split.append((n + 1, 1599))
        split.extend([(1599 + round(i * m), 1599 + round((i + 1) * m))
                      for i in range(num_workers - 1)])
    else:
        m = (total - n) / num_workers
        split.extend([(n + 1 + round(i * m), n + 1 + round((i + 1) * m))
                      for i in range(num_workers)])

    return split


def crawl_messages(driver, wait, rng):
    if rng[1] == 1599:
        date = '20160609'
        if rng[0] == 1:
            start = '2011/11/18 07:43:44'
        else:
            output = File_manager('raw', 'disasterMessage')
            start = read_csv(output.path).iloc[-1]['time']

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
    else:
        while True:
            total = int(driver.find_element_by_id('totCnt').text)
            page_input = driver.find_element_by_id('bbs_page')
            page_input.clear()
            page_input.send_keys((total - rng[0]) // 10 + 1)
            check_stale = driver.find_element_by_id('bbs_tr_0_bbs_title')
            driver.find_element_by_class_name('go_btn').click()
            wait.until(EC.staleness_of(check_stale))

            total = int(driver.find_element_by_id('totCnt').text)
            i = (total - rng[0]) % 10
            res = int(driver.find_element_by_id(f'bbs_tr_{i}_num_td').text)

            if rng[0] == res:
                driver.find_element_by_id(f'bbs_tr_{i}_bbs_title').click()
                break
            else:
                continue

    messages = {'time': [], 'body': [], 'to': []}
    for _ in range(*rng):
        parse_messages(driver, wait, messages)

    return DataFrame(messages)


def parse_messages(driver, wait, messages):
    title = wait.until(Message_loaded((By.ID, 'sj')))

    time = title.text[:-5]
    text = driver.find_element_by_id('cn').text.split('-송출지역-')
    body = text[0].strip().replace('\n', ' ')
    if len(text) == 1:
        to = None
    else:
        to = text[1].strip().replace('\n', ', ')

    messages['time'].append(time)
    messages['body'].append(body)
    messages['to'].append(to)

    try:
        next = wait.until(Message_loaded((By.ID, 'bbs_next')))
        next.click()
    except TimeoutException:
        return


def worker(rng, options, lst):
    with webdriver.Chrome(options=options) as driver:
        wait = WebDriverWait(driver, 5)
        init_page(driver, wait)
        lst.append(crawl_messages(driver, wait, rng))


@print_time
def disaster_message_crawler(max_workers):
    total = None
    minimum = 10
    message_list = []
    output = File_manager('raw', 'disasterMessage')
    n = int(output.parse_version()['disasterMessage'])

    options = webdriver.ChromeOptions()
    prefs = {'profile.default_content_setting_values': {
        'cookies': 2, 'images': 2, 'plugins': 2, 'popups': 2, 'geolocation': 2,
        'notifications': 2, 'auto_select_certificate': 2, 'fullscreen': 2,
        'mouselock': 2, 'mixed_script': 2, 'media_stream': 2,
        'media_stream_mic': 2, 'media_stream_camera': 2,
        'protocol_handlers': 2, 'ppapi_broker': 2, 'automatic_downloads': 2,
        'midi_sysex': 2, 'push_messaging': 2, 'ssl_cert_decisions': 2,
        'metro_switch_to_desktop': 2, 'protected_media_identifier': 2,
        'app_banner': 2, 'site_engagement': 2, 'durable_storage': 2
        }}
    options.add_experimental_option('prefs', prefs)
    options.add_argument('disable-infobars')
    options.add_argument('--disable-extensions')
    options.add_argument('--headless')

    with webdriver.Chrome(options=options) as driver:
        wait = WebDriverWait(driver, 5)
        init_page(driver, wait)

        total = int(driver.find_element_by_id('totCnt').text)

        if total == n:
            print('Collected 0 new item.')
            return

        num_workers = min(max((total - n) // minimum, 1), max_workers)
        split = get_split(total, n, num_workers)

        with Manager() as manager:
            lst = manager.list()
            workers = [Process(target=worker, args=(split[i], options, lst))
                       for i in range(1, num_workers)]

            for wrkr in workers:
                wrkr.start()

            message_list.append(crawl_messages(driver, wait, split[0]))

            for wrkr in workers:
                wrkr.join()

            message_list.extend(lst)

    params = {
        'mode': 'a' if n else 'w',
        'header': False if n else True,
        'index': False
        }
    concat(message_list).sort_values(by='time').to_csv(output.path, **params)
    output.update_version({'disasterMessage': str(total)})
    print(f'Collected {total - n} new items.')

import json
import random
import time
import requests
import re
from datetime import datetime
import urllib3
from requests.packages.urllib3.exceptions import ProtocolError

from watcher.bricks import BaseThread


class SeismInfo:

    mondict = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07',
        'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }

    include_key_words = ['北纬', '东经', '中国地震台网', '地震快讯', '测定']

    def __init__(self, m_id, m_text, m_date):
        self._mblog_id = m_id
        self._mblog_text = m_text
        self._mblog_date = self.format_date(m_date)

    def format_date(self, m_date):
        param_list = m_date.split(' ')
        date = '{}-{}-{} {}'.format(param_list[-1], self.mondict[param_list[1]], param_list[2], param_list[3])
        return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

    def is_seism_forecast(self):
        return True
        # return sum(list(map(lambda x: self._mblog_text.find(x) != -1, SeismInfo.include_key_words))) == len(SeismInfo.include_key_words)

    @property
    def id(self):
        return self._mblog_id

    @property
    def text(self):
        return self._mblog_text

    @property
    def date(self):
        return self._mblog_date

    def __lt__(self, other):
        return self._mblog_date > other.date

    def __eq__(self, other):
        return self._mblog_date == other.date


class WeiboSpider(BaseThread):

    def __init__(self, time_interval):
        super(WeiboSpider, self).__init__()
        self.req = requests.Session()
        self.page = 1
        self.headers = {
            "user-agent": "'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15"
                          " (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1'",
            'Connection': 'close'
        }
        self.value_id = 5680719858
        self.container_id = 1076035680719858
        self.since_id = None
        self.clean = re.compile('<.*?>')
        self.url = f"https://m.weibo.cn/api/container/getIndex?type=uid&value={self.since_id}" \
                          f"&containerid={self.container_id}&since_id={self.since_id}"
        self.network_retry_time = 1 * 60
        self.time_interval_lower = time_interval
        self.time_interval_upper = 3 * time_interval
        self.time_anti_crawling = 7 * 60

    def run(self):
        last_id = None

        while self.should_keep_running():

            while True:
                try:
                    url = f"https://m.weibo.cn/api/container/getIndex?type=uid&value={self.since_id}" \
                          f"&containerid={self.container_id}&since_id={self.since_id}"
                    res = self.req.get(url, headers=self.headers)
                    r = json.loads(res.text)
                except TimeoutError or ProtocolError or ConnectionError as e:
                    print(e)
                    time.sleep(1 * 60)
                    continue

                if 'since_id' not in r['data']['cardlistInfo']:
                    self.since_id = None
                else:
                    self.since_id = r['data']['cardlistInfo']['since_id']

                results = list()
                for item in r['data']['cards']:
                    if item['card_type'] == 9:
                        item = item['mblog']
                        results.append(SeismInfo(item['id'], item['text'], item['created_at']))
                results = sorted(results)

                if len(results) != 0:
                    last_id = results[0].id
                    break

            self.since_id = None
            counter = 0
            tmp_id = None
            while True:
                try:
                    url = f"https://m.weibo.cn/api/container/getIndex?type=uid&value={self.since_id}" \
                          f"&containerid={self.container_id}&since_id={self.since_id}"
                    res = self.req.get(url, headers=self.headers)
                    r = json.loads(res.text)
                except TimeoutError or ProtocolError or ConnectionError as e:
                    print(e)
                    time.sleep(15 * 60)
                    continue

                if r['ok'] == 1:
                    if 'since_id' not in r['data']['cardlistInfo']:
                        self.since_id = None
                    else:
                        self.since_id = r['data']['cardlistInfo']['since_id']

                    results = list()
                    for item in r['data']['cards']:
                        if item['card_type'] == 9:
                            item = item['mblog']
                            if item['isLongText']:
                                url = f"https://m.weibo.cn/statuses/extend?id={item['id']}"
                                res = self.req.get(url, headers=self.headers)
                                r = json.loads(res.text)
                                if 'ok' in r and r['ok'] == 1:
                                    item['text'] = r['data']['longTextContent']
                            results.append(SeismInfo(item['id'], item['text'], item['created_at']))
                    results = sorted(results)

                    for item in results:
                        if counter == 0:
                            tmp_id = item.id

                        if item.id == last_id:
                            last_id = tmp_id
                            self.since_id = None
                            counter = 0
                            time.sleep(random.randint(self.time_interval, 3 * 60))
                            break

                        counter += 1
                        if item.is_seism_forecast():
                            print(str(item.date) + ': ' + re.sub(self.clean, '', item.text) + '\n')
                            self.compute_intensity()
                elif r['ok'] == 0:
                    time.sleep(random.randint(self.time_interval, 7 * 60))

    def compute_intensity(self):
        pass


if __name__ == '__main__':
    t = WeiboSpider(60)
    t.start()
    t.join()

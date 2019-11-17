import requests
from http.client import RemoteDisconnected
from bs4 import BeautifulSoup
import re
import sys
import json
import time
import warnings
import logging
from dictionary import dictctrl

LOG_LEVEL = logging.DEBUG

class webscrapper():
    """[summary]
    Webからリストを取得し、master_dictを更新する
    """
    warnings.simplefilter(
        'ignore', requests.urllib3.exceptions.InsecureRequestWarning)            
    logging.basicConfig(filename='./log/sake.log', level=LOG_LEVEL)


    # web_nameリスト
    WEB_SAKETIMES = 'SAKETIMES'      

    # SAKETIMES情報
    SAKETIMES_URL = 'https://jp.sake-times.com/sakagura'
    SAKETIMES_FILE = './dict/saketimes/dict_saketimes.json'
    SAKETIMES_FILE_ERR = './dict/saketimes/dict_saketimes_err.log'
    IS_SAKETIMES_NEED = False

    def __init__(self, web_name):
        """[summary]
         指定したwebからリストを取得し、dictを更新する。\n
        新規 meigara は追加\n
        同一 meigara の場合は更新\n

        Arguments:\n
            fp_master_dict {[str]} -- [書き込み先辞書のファイルパス]\n
            web_name {[str]} -- [対象web名, ex. SAKETIMES]
        """        
        self.web_name = web_name

    def writeSAKETIMES2dict(self):
        """[summary]
        一時ファイル(sakedict)からdictファイルに書き込み
        """  
        print('writeSAKETIMES2dict start')
        with open(self.SAKETIMES_FILE, mode = 'r', encoding='utf-8') as fr:
            saketimesd = json.load(fr)
            print(saketimesd.keys())
            fr.close

        show = True
        sake_list_dict = []
        for area in saketimesd:
            for prefecture in saketimesd[area]:
                for sakagura in saketimesd[area][prefecture]['sakagura']:
                    url = saketimesd[area][prefecture]['sakagura'][sakagura]['url']
                    meigara = saketimesd[area][prefecture]['sakagura'][sakagura]['meigara']
                    sakagura_name =saketimesd[area][prefecture]['sakagura'][sakagura]['sakagura_name']
                    sakagura_name_yomi =saketimesd[area][prefecture]['sakagura'][sakagura]['sakagura_name_yomi']
                    meigara_yomi =saketimesd[area][prefecture]['sakagura'][sakagura]['meigara_yomi']
                    homepage = saketimesd[area][prefecture]['sakagura'][sakagura]['homepage']

                    item = {
                        'area': area,
                        'prefecture': prefecture,
                        'url':url,
                        'meigara': meigara,
                        'meigara_yomi': meigara_yomi,
                        'sakagura_name': sakagura_name,
                        'sakagura_name_yomi': sakagura_name_yomi,
                        'homepage': homepage
                    }
                    sake_list_dict.append(item)

#                    if show:
#                        print(item)
#                        show = False

        logging.info(f'aws dynamo[SAKE_LIST] batchupdate start. record = {len(sake_list_dict)}')
        db = dictctrl()
        db.batchupdate(sake_list_dict)

        


    def getWeb(self):

        if self.web_name == webscrapper.WEB_SAKETIMES:
            if webscrapper.IS_SAKETIMES_NEED:
                self.getSAKETIMES
            else:
               logging.info(f'scrap web({webscrapper.WEB_SAKETIMES}) is skipped.') 


    def getSAKETIMES(self):
        """[summary]
        SAKETIMESからスクラップし、一時ファイル(SAKETIMES_FILE)にデータ掃き出し.
        """            
        print(f'** start {sys._getframe().f_code.co_name}')

        # getSakeDictFromWebから呼び出される.

        # 本関数内のdictionary. ./dict/saketimes/dict_saketimes.txtに書き込むまでのテンポラリ.
        d = dict()

        # 登録数カウンター
        count_area = 0
        count_prefecture = 0
        count_sakagura = 0

        # topページアクセス
        try:
            r = requests.get(webscrapper.SAKETIMES_URL, verify=False)
            bf_top = BeautifulSoup(r.text, 'html.parser')
        except RemoteDisconnected as e:
            print(
                f'** RemoteDisconnected Exeption occured while get {webscrapper.SAKETIMES_URL}')
            print(e.strerror)
            logging.warning(f'{time.localtime}: ** RemoteDisconnected Exeption occured while get {webscrapper.SAKETIMES_URL}')
            return

        # area_list 読み込み
        area_list = bf_top.select('span.area-title')

        for a in area_list:
            # カウンター
            count_area += 1

            # area_name 整形/設定
            area_name = re.sub(r'\(.*$', '', a.text)
            d.setdefault(area_name, {})

            # 経過表示
            print(f' + Area: {area_name}')

            # areaリストからprefecture_list 読み込み
            area_root = a.parent.parent
            prefecture_list = area_root.select('li')
            if len(prefecture_list) == 0:
                print(f' -- no prefecture in area[{area_name}] : skip')
                continue

            for p in prefecture_list:
                # カウンター
                count_prefecture += 1

                # prefecture_name 整形/設定
                prefecture_name = re.sub(r'（.*$', '', p.text)
                d[area_name].setdefault(prefecture_name, {})

                # 経過表示
                print(f' -- progress: {prefecture_name}')

                # prefecture_url 読み込み/設定
                prefecture_url = p.find('a')['href']
                d[area_name][prefecture_name].setdefault('url', prefecture_url)

                # prefecture url アクセス
                try:
                    r = requests.get(prefecture_url, verify=False)
                    bf_sakagura_top = BeautifulSoup(
                        r.text, 'html.parser').find('table')
                except RemoteDisconnected as e:
                    print(
                        f'** RemoteDisconnected Exeption occured while get {prefecture_url}')
                    print(f'** SKIP prefecture {prefecture_name}')
                    logging.warning(f'{time.localtime}: ** RemoteDisconnected Exeption occured while get {prefecture_url}')
                    continue

                # sakagura 設定
                d[area_name][prefecture_name].setdefault('sakagura', {})

                # sakagura_list 読み込み
                try:
                    sakagura_list = bf_sakagura_top.select('span.main')
                    if len(sakagura_list) == 0:
                        print(
                            f' -- no sakagura in prefecture[{prefecture_name}] : skip')
                        continue
                except:
                    print(f'no sakagura in {prefecture_name}')
                    continue

                # sakagura内カウンター
                count_sakagura_in_prefecture = 0

                for s in sakagura_list:
                    # カウンター
                    count_sakagura += 1
                    count_sakagura_in_prefecture += 1

                    # sakagura_name 読み込み/設定
                    d[area_name][prefecture_name]['sakagura'].setdefault(
                        s.text, {})

                    # sakagura_url 読み込み/設定
                    sakagura_url = s.find('a')['href']
                    d[area_name][prefecture_name]['sakagura'][s.text].setdefault(
                        'url', sakagura_url)

                    # meigara 読み込み/設定
                    b = s.parent
                    meigara = b.find('dd').text
                    d[area_name][prefecture_name]['sakagura'][s.text].setdefault(
                        'meigara', meigara)

                    # sakaguraアクセス
                    # Webアクセス負荷軽減
                    time.sleep(0.5)

                    try:
                        r = requests.get(sakagura_url, verify=False)
                        bf_sakagura = BeautifulSoup(r.text, 'html.parser')
                    except RemoteDisconnected as e:
                        print(
                            f'** RemoteDisconnected Exeption occured while get {sakagura_url}')
                        print(f'** SKIP sakagura. {s.text}')
                        logging.warning(f'{time.localtime}: ** RemoteDisconnected Exeption occured while get {sakagura_url}')
                        continue

                    # sakagura_name 読み込み/設定
                    sakagura_name = bf_sakagura.select('span.main')[0].text
                    d[area_name][prefecture_name]['sakagura'][s.text].setdefault(
                        'sakagura_name', sakagura_name)

                    # sakagura_name_yomi 読み込み/設定
                    sakagura_name_yomi = bf_sakagura.select('span.sub')[0].text
                    d[area_name][prefecture_name]['sakagura'][s.text].setdefault(
                        'sakagura_name_yomi', sakagura_name_yomi)

                    # meigara 読み込み/設定
                    meigara = bf_sakagura.select('span.main')[1].text
                    d[area_name][prefecture_name]['sakagura'][s.text].setdefault(
                        'meigara', meigara)

                    # meigara 読み込み/設定
                    meigara_yomi = bf_sakagura.select('span.sub')[1].text
                    d[area_name][prefecture_name]['sakagura'][s.text].setdefault(
                        'meigara_yomi', meigara_yomi)

                    # homepage 読み込み/設定
                    homepage = bf_sakagura.select('td.ellipsis')[1].text
                    d[area_name][prefecture_name]['sakagura'][s.text].setdefault(
                        'homepage', homepage)

                    # 経過表示
                    if count_sakagura % 10 == 0:
                        print(
                            f' -- counter: {prefecture_name} {count_sakagura_in_prefecture}, total= {count_sakagura}')

        # 結果表示
        print(f'++ scrapping from {self.web_name} end. total= {count_sakagura} sakelist!')

        # SAKETIMES読み込み完了. 一時ファイルに書き込み.
        with open(webscrapper.SAKETIMES_FILE, mode='w') as fw:
            json.dump(d, fw)
            print('+ write sakedict.txt complete')
            fw.close


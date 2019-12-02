import requests
from http.client import RemoteDisconnected
from bs4 import BeautifulSoup
import re
import sys
import json
import time
import warnings
from webscrap import webscrapper
from dictionary import dictctrl

MASTER_DICT_FILE = './dict/master/sake_dict.txt'

## Handler Actions.
# web sourceからDB更新 
UPDATE_DB_FROM_WEB = 'updatedictionary'
# DBへitem登録(単発)
UPDATE_DB_BY_ITEM = 'updateitem'

READ_SAKETIMES = False


def sake_handler(event, context):
    print(f'** start {sys._getframe().f_code.co_name}')

    if event['event'] == 'query':
        queryoption = event['option']
        print(type(queryoption))
        db = dictctrl()
        db.query(queryoption)

    elif event['event'] == 'updatedictionary':
        if event['option']['source'] == webscrapper.WEB_SAKETIMES:
            print('webscrap start: SAKETIMES')
            scrapper = webscrapper(webscrapper.WEB_SAKETIMES)
            scrapper.writeSAKETIMES2dict()

    elif event['event'] == 'updateitem':
        pass

    else:
        print('sake_handler. no action')



def writeErrorLog(filepath, message):
    with open(filepath, mode='a') as fa:
        fa.write(message)
        fa.close


if __name__ == "__main__":
    print(f'** start {sys._getframe().f_code.co_name}')

    # SAKETIMESからの辞書更新
    # 2回目やるとawsでduplicateエラーになるので
    # 新規だけ更新するのは一工夫必要.
    """
    event = {
        'event': 'updatedictionary',
        'option': {
            'source': webscrapper.WEB_SAKETIMES
        }
    }
    """

    # クエリテスト
    event = {
        'event': 'query',
        'option': {
            'action': 'select',        # アクション
            'query': '奥の松',        # 検索語(str)
            'limit': 1               # 検索上限(int)
        }
    }    

    sake_handler(event, None)

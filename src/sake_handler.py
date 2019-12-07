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

# AWS(from DiflogFlow) Intents
# 銘柄から県、酒蔵を答える
ASK_PLACE_FROM_SAKENAME = "ask_place_from_sake"

# Handler Actions.
# web sourceからDB更新
UPDATE_DB_FROM_WEB = 'updatedictionary'
# DBへitem登録(単発)
UPDATE_DB_BY_ITEM = 'updateitem'

READ_SAKETIMES = False


def sake_handler(event, context):
    print(f'** start {sys._getframe().f_code.co_name}')

    ev = dict()
    if "mode" in event:
        if event["mode"]=="local":
            print("DEBUG mode. queryは固定値")
            ev=event["body"]
        elif event["mode"]=="aws":
            ev=json.loads(event["body"])      
    else:
        print("AWS mode. queryはDialogFlowから受信")
        ev=json.loads(event["body"])

    print('input event parameter', ev)
    intent = ev["queryResult"]["intent"]["displayName"]

    if intent == ASK_PLACE_FROM_SAKENAME:
        message = askPlaceFromSake(ev)
        res = makeResponse(message)
        return res

    if event['event'] == 'query':
        queryoption = event['option']
        print(type(queryoption))

    elif event['event'] == 'updatedictionary':
        if event['option']['source'] == webscrapper.WEB_SAKETIMES:
            print('webscrap start: SAKETIMES')
            scrapper = webscrapper(webscrapper.WEB_SAKETIMES)
            scrapper.writeSAKETIMES2dict()

    elif event['event'] == 'updateitem':
        pass

    else:
        print('sake_handler. no action')


def makeResponse(resmessage):
    """[summary]

    Arguments:
        resmessage {str} -- [dialogFlowに返すメッセージ(text)]

    Returns:
        [json] -- [handlerリクエストに対するdialogFlowへのレスポンス]
    """
    return {
        'statusCode': 200,
        'body': json.dumps({
            "fulfillment_text": "show text from aws lambda",
            "fulfillment_messages": [{
                "text": {
                    "text": [
                        resmessage,
                    ]
                }
            }]
        })
    }


def writeErrorLog(filepath, message):
    print(f'** start {sys._getframe().f_code.co_name}')
    with open(filepath, mode='a') as fa:
        fa.write(message)
        fa.close


def askPlaceFromSake(ev):
    print(f'** start {sys._getframe().f_code.co_name}')
    message = "return message default"
    sakename = "query meigara default"

    if "queryResult" in ev:
        if "parameters" in ev["queryResult"]:
            if "sakename" in ev["queryResult"]["parameters"]:
                    sakename = ev["queryResult"]["parameters"]["sakename"]
    else:
        print('no query(sakename).. set default as 〆張鶴')
        sakename = "〆張鶴"

    # DB検索
    db = dictctrl()
    queryOptin ={
        'query': sakename,  # 検索語(str)
        'key': db.KEY_GSI_MEIGARA,  # 検索キー(str)
        'limit': 1,         # 検索上限(int)
    }

    print('query option', queryOptin)
    res = db.query(queryOptin)

    # 検索ヒット数
    if len(res) > 0:
        count = res["Count"]
        items = res["Items"]
    else:
        count = 0
    
    if count == 1:
        # 1個だけだったら件名、酒造を返す.
        prefecture = items[0][dictctrl.KEY_OTHER_PREFECTURE]
        sakagura = items[0][dictctrl.KEY_OTHER_SAKAGURA_NAME]
        message = f"{sakename}は、{prefecture}、{sakagura}のお酒です"
    elif count > 1:
        # 複数あったら1個だけ回答。他に何県にあるか返す.
        prefecture = items[0][dictctrl.KEY_OTHER_PREFECTURE]
        sakagura = items[0][dictctrl.KEY_OTHER_SAKAGURA_NAME]
        other_prefecture = ""
        for index, item in enumerate(items):
            if index == 0:
                continue
            else:
                other_prefecture += f"{item[dictctrl.KEY_OTHER_PREFECTURE]}、\n"
        message = f"{sakename}は、{prefecture}、{sakagura}のお酒です。\n\n他にも同じ名前のお酒が{other_prefecture}にあります。"
    else:
        # ヒットしなかったときのメッセージ.
        message = f"{sakename}はまだ知らないお酒ですね。"

    return message


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
        "mode":"local",
        "body": {
            "responseId": "DEBUG",
            "queryResult": {    
                "queryText": "田酒はどこのお酒？",
                "parameters": {
                    "sakename": "田酒"
                },
                "allRequiredParamsPresent": "true",
                "intent": {      
                    "name": "projects/ponsyuchan-spytgh/agent/intents/c93fe1d6-2764-4468-bbf4-e612c7a3a88a",   
                    "displayName": "ask_place_from_sake"
                }
            }
        }
    }

    sake_handler(event, None)

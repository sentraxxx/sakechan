import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import sys


class dictctrl():

    TABLE_SAKELIST = 'SAKE_LIST'

    # SAKE_LISTのKEY
    # primary key
    KEY_PREFECTURE = "prefecture"

    # Global secondery Index
    KEY_GSI_MEIGARA = "meigara"  # key
    INDEX_GSI_MEIGARA = "meigara-index"  # GSI

    KEY_GSI_MEIGARA_YOMI = "meigara_yomi"
    INDEX_GSI_MEIGARA_YOMI = "meigara_yomi-index"

    # キー以外の項目
    KEY_OTHER_PREFECTURE = "prefecture"
    KEY_OTHER_SAKAGURA_NAME = "sakagura_name"
    KEY_OTHER_SAKAGURA_NAME_YOMI = "sakagura_name_yomi"
    KEY_OTHER_URL = "url"

    def __init__(self):
        """[summary]
        aws dynamodb(SAKE_LIST)にアクセスするクラス.
        """
        self.dict = dict

        self.db = boto3.resource('dynamodb', verify=False)
        print('db type= ', type(self.db))
        self.table: boto3.dynamodb.table = self.db.Table(dictctrl.TABLE_SAKELIST)
        print("Table status:", self.table.table_status)

    def batchupdate(self, items: list):
        """ item(dictionary)をdynamodbにupdateする.
        itemの必須項目は('prefecture'(PRYMARY_KEY),'meigara'(SORT_KEY) )
        """

        counter = 0
        batchlist = []
        with self.table.batch_writer() as batch:
            for it in items:
                if (it['meigara'], it['prefecture']) in batchlist:
                    print(f'batch_write duplicate item: {it}')
                    continue
                else:
                    batchlist.append((it['meigara'], it['prefecture']))

                    # print(f'item[{counter}]=', it)
                    batch.put_item(
                        Item=it
                    )
                    counter += 1
                    if counter % 50 == 0:
                        print('aws dynamodb update.. count= ', counter)
        return counter

    def update(self, item):
        """[summary]
        itemをaws SAKE_LISTに追加更新する.

        Arguments:
            item {[dict]} -- [must 'prefecture', 'meigara']
        """
        print('update')

    def query(self, queryoptions):
        """[summary]

        Arguments:
            **queryoptions{[dict]} -- [クエリ―オプション\n
             - key: 検索キー \n
             - query: 検索語 \n
             - limit(option): 検索数]

        Returns:
            dict -- [SAKE_LIST検索結果. dict配列]
        """
        print(f'** start {sys._getframe().f_code.co_name}')

        # queryoptionsから条件抽出、設定
        print(queryoptions) 

        conditions = dict()
        if "query" in queryoptions and "key" in queryoptions:
            conditions.setdefault("KeyConditionExpression", Key(dictctrl.KEY_GSI_MEIGARA).eq(queryoptions["query"]))
        if "key" in queryoptions:
            if queryoptions["key"] == dictctrl.KEY_GSI_MEIGARA:
                conditions.setdefault("IndexName",dictctrl.INDEX_GSI_MEIGARA)
            elif queryoptions["key"] == dictctrl.KEY_GSI_MEIGARA_YOMI:
                conditions.setdefault("IndexName", dictctrl.INDEX_GSI_MEIGARA_YOMI)
        if "limit" in queryoptions:
            conditions.setdefault("Limit", queryoptions["limit"])

        # queryテスト
        # response = self.table.query(KeyConditionExpression=Key('prefecture').eq('青森県') & Key('meigara').eq('田酒'))
        response = self.table.query(**conditions)

        print(f'response = {response}')
        return response
        

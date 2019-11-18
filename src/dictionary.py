import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

class dictctrl():

    def __init__(self):
        """[summary]
        aws dynamodb(SAKE_LIST)にアクセスするクラス.
        """
        self.dict = dict
        TABLE_SAKELIST = 'SAKE_LIST'

        self.db = boto3.resource('dynamodb')
        print('db type= ',type(self.db))
        self.table: boto3.dynamodb.table = self.db.Table(TABLE_SAKELIST)
        print("Table status:", self.table.table_status)


    def batchupdate(self, items:list):    
        """ item(dictionary)をdynamodbにupdateする.
        itemの必須項目は('prefecture'(PRYMARY_KEY),'meigara'(SORT_KEY) )
        """

        counter = 0
        with self.table.batch_writer() as batch:
            for it in items:
                print('counter = ', counter)
                print(f'item[{counter}]=', it)
                batch.put_item(
                    Item= it
                )
                counter +=1
    
    def update(self, item):
        """[summary]
        itemをaws SAKE_LISTに追加更新する.

        Arguments:
            item {[dict]} -- [must 'prefecture', 'meigara']
        """        
        print('update')

    def query(self, **queryoptions) -> dict:
        """[summary]
        
        Arguments:
            **queryoptions{[dict]} -- [クエリ―オプション\n
             - ]

        Returns:
            dict -- [description]
        """        

        print('query start')



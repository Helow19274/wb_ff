import logging

import requests
from datetime import datetime, timezone, timedelta


class WildBerries:
    def __init__(self, token):
        self.base_url = 'https://suppliers-api.wildberries.ru/api/v2/'

        self.session = requests.Session()
        self.session.headers = {'Authorization': token}

        self.products = {product['barcode']: product for product in self.get_products()}

    def get_orders(self, date_start=(datetime.now(timezone.utc) - timedelta(days=14)).isoformat(), skip=0):
        payload = {
            'date_start': date_start,
            'status': 0,
            'take': 1000,
            'skip': skip
        }

        data = self.session.get(url=f'{self.base_url}orders', params=payload).json()
        if data['total'] > 1000:
            return data['orders'] + self.get_orders(date_start, skip + 1000)
        else:
            return data['orders']

    def get_products(self, skip=0):
        payload = {
            'take': 1000,
            'skip': skip
        }

        r = self.session.get(url=f'{self.base_url}stocks', params=payload)
        if r.status_code == 401:
            logging.error('Неверный ключ WB')
            exit()

        data = r.json()
        if data['total'] > 1000:
            return data['stocks'] + self.get_products(skip + 1000)
        else:
            return data['stocks']

    def update_order_status(self, order_id, status=1):
        payload = [{
            'orderId': order_id,
            'status': status
        }]

        r = self.session.put(url=f'{self.base_url}orders', json=payload)
        return r.ok

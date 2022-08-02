import logging
import time
import config
import pprint
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class CdekSDK:
    def __init__(self):
        self.base_url = 'https://api.cdek.ru/v2/'
        self.client = requests.Session()

        payload = {
            'client_id': config.CDEK_PUBLIC_KEY,
            'client_secret': config.CDEK_SECRET_KEY,
            'grant_type': 'client_credentials'
        }
        try:
            token = self.client.post(f'{self.base_url}oauth/token', data=payload).json()['access_token']
        except Exception:
            logging.error('Неверные ключи СДЭК')
            exit()

        self.client.headers = {'Authorization': f'Bearer {token}'}

    def create_order(self, order):
        payload = {
            'number': order['orderUID'][:40],
            'tariff_code': 11,
            'shipment_point': config.SEND_PVZ,
            'recipient': {
                'name': order['userInfo']['fio'],
                'phones': [{'number': order['userInfo']['phone']}]
            },
            'to_location': {
                'longitude': order['deliveryAddressDetails']['longitude'],
                'latitude': order['deliveryAddressDetails']['latitude'],
                'region': order['deliveryAddressDetails']['province'],
                'city': order['deliveryAddressDetails']['city'],
                'address': order['deliveryAddress']
            },
            'packages': [{
                'number': 1,
                'weight': config.WEIGHT,
                'length': 10,
                'width': 10,
                'height': 10,
                'items': []
            }]
        }

        total_products = sum(product['count'] for product in order['products'].values())

        for barcode, product in order['products'].items():
            payload['packages'][0]['items'].append({
                'name': product['name'],
                'ware_key': barcode,
                'payment': {
                    'value': 0,
                },
                'cost': 0,
                'weight': round(config.WEIGHT / total_products),
                'amount': product['count']
            })

        calculator_payload = payload.copy()
        calculator_payload['from_location'] = {
            'code': 270
        }
        r = self.client.post(f'{self.base_url}calculator/tarifflist', json=calculator_payload).json()['tariff_codes']
        available_tariffs = [tariff['tariff_code'] for tariff in r]
        if 137 in available_tariffs:
            payload['tariff_code'] = 137
        elif 233 in available_tariffs:
            payload['tariff_code'] = 233

        uuid = self.client.post(f'{self.base_url}orders', json=payload).json()['entity']['uuid']
        time.sleep(1)
        order_info = self.client.get(f'{self.base_url}orders/{uuid}').json()['entity']
        if not (tracking_number := order_info.get('cdek_number')):
            error = pprint.pformat(order_info['requests'][0]['errors'])
            logging.error(f'Не удалось создать заказ по ошибке: \n{error}')
            return False
        else:
            logging.info(f'Заказ создан, трек-номер: {tracking_number}')
            return True


class CdekFF:
    def __init__(self):
        self.base = 'https://cdek.orderadmin.ru/api/'

        self.client = requests.Session()
        self.client.auth = (config.FF_PUBLIC_KEY, config.FF_SECRET_KEY)

        shops = [str(shop['id']) for shop in self.method('products/shops')['_embedded']['shops']]
        if config.FF_SHOP not in shops:
            logging.error('Указан неверный магазин ФФ')
            exit()
        warehouses = [str(warehouse['id']) for warehouse in self.method('storage/warehouse')['_embedded']['warehouse']]
        if config.FF_WAREHOUSE not in warehouses:
            logging.error('Указан неверный склад ФФ')
            exit()
        senders = [str(sender['id']) for sender in self.method('delivery-services/senders')['_embedded']['senders']]
        if config.FF_SENDER not in senders:
            logging.error('Указан неверный отправитель ФФ')
            exit()

        payload = {
            'filter[0][type]': 'eq',
            'filter[0][field]': 'shop',
            'filter[0][value]': config.FF_SHOP
        }
        products = self.get_all('products/offer', 'product_offer', payload)
        self.products = {product[config.FF_FIELD]: product for product in products}

    def create_order(self, order):
        payload = {
            'shop': config.FF_SHOP,
            'extId': order['orderUID'][:31],
            'paymentState': 'paid',
            'profile': {
                'name': order['userInfo']['fio'],
            },
            'phone': order['userInfo']['phone'],
            'eav': {
                'order-reserve-warehouse': config.FF_WAREHOUSE
            },
            'deliveryRequest': {
                'deliveryService': 1,
                'retailPrice': 0,
                'estimatedCost': 0,
                'rate': 49,
                'sender': config.FF_SENDER
            },
            'orderProducts': []
        }

        index = order['deliveryAddress'].split()[-1]
        filter = {
            'filter[0][type]': 'eq',
            'filter[0][field]': 'extId',
            'filter[0][value]': index
        }
        postcode = self.method('delivery-services/postcodes', filter)['_embedded']['postcodes']
        if not postcode:
            logging.error(f'В базе ФФ не найден индекс {index}, отмена создания заказа')
            return False

        locality = postcode[0]['_embedded']['locality']['id']
        payload['address'] = {
            'locality': locality,
            'postcode': index,
            'street': order['deliveryAddressDetails']['street'],
            'house': order['deliveryAddressDetails']['home'],
            'apartment': order['deliveryAddressDetails']['flat'],
        }

        for barcode, product in order['products'].items():
            ff_product = self.products.get(barcode)
            if not ff_product:
                logging.error(f'В ФФ не найден товар {barcode}, отмена создания заказа')
                return False

            payload['orderProducts'].append({
                'productOffer': ff_product['id'],
                'shop': config.FF_SHOP,
                'count': product['count'],
                'price': product['price']
            })

        payload['orderPrice'] = sum(product['price'] * product['count'] for product in payload['orderProducts'])
        payload['totalPrice'] = payload['orderPrice']

        r = self.method('products/order', payload, 'POST')
        if r:
            logging.info(f'Создан заказ, его номер: {r["id"]}')
            return True
        logging.error('Не удалось создать заказ')
        return False

    def get_all(self, method, field, payload):
        payload = payload.copy() if payload is not None else {}
        payload['page'] = 1
        if 'per_page' not in payload:
            payload['per_page'] = 250

        objects = []
        res = self.method(method, payload)
        objects.extend(res['_embedded'][field])

        if res['page_count'] > 1:
            tasks = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                for page in range(2, res['page_count'] + 1):
                    l_payload = payload.copy()
                    l_payload['page'] = page
                    tasks.append(executor.submit(self.method, method=method, payload=payload))
                for task in as_completed(tasks):
                    objects.extend(task.result()['_embedded'][field])

        return objects

    def method(self, method, payload=None, m='GET'):
        if m in ['POST', 'PATCH']:
            r = self.client.request(m, f'{self.base}{method}', json=payload)
        else:
            r = self.client.get(f'{self.base}{method}', params=payload)

        if r.status_code == 401:
            logging.error('Указаны неверные ключи ФФ')
            exit()
        else:
            if not r.ok:
                try:
                    json = r.json()
                    logging.error(f'Ошибка создания заказа: {json}')
                except Exception:
                    logging.error('Ошибка создания заказа')
                return

        return r.json()

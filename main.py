import json
import logging
import config
import os.path
from wb_sdk import WildBerries
from collections import defaultdict
from cdek_sdk import CdekSDK, CdekFF

logging.basicConfig(filename='log.log', level=logging.DEBUG)


def main():
    wb = WildBerries(config.WILD_TOKEN)

    if config.TYPE == 'LOGISTICS':
        client = CdekSDK()
    elif config.TYPE == 'FF':
        client = CdekFF()

    with open('data.json') as f:
        try:
            data = json.load(f)
        except Exception:
            logging.error('Неверный формат файла data.json')
            exit()

    orders = list(filter(lambda order: order['deliveryType'] == 2 and order['userStatus'] == 4, wb.get_orders()))
    logging.info(f'Сборочных заданий: {len(orders)}')

    grouped_orders = defaultdict(list)
    for order in orders:
        grouped_orders[order['orderUID']].append(order)
    logging.info(f'Заказов: {len(grouped_orders)}')

    for uid, orders in grouped_orders.items():
        if any(order['orderId'] in data['processed'] for order in orders):
            logging.warning(f'''Заказ {uid}, в который входят сборочные задания {", ".join(order["orderId"] for order in orders)} уже обработан, пропускаю.
Пожалуйста, обратите на него внимание (возможно, его нужно перевести в другой статус в ЛК ВБ''')
            continue

        logging.info(f'Заказ {uid}, в который входят сборочные задания {", ".join(order["orderId"] for order in orders)}')
        main_order = orders[0]
        main_order['products'] = {}
        for order in orders:
            if order['barcode'] not in main_order['products']:
                main_order['products'][order['barcode']] = {
                    'name': wb.products[order['barcode']]['name'],
                    'price': order['totalPrice'] / 100,
                    'count': 1
                }
            else:
                main_order['products'][order['barcode']]['count'] += 1

        if client.create_order(main_order):
            for order in orders:
                wb.update_order_status(order['orderId'])

            with open('data.json', 'w') as f:
                data['processed'].extend(order['orderId'] for order in orders)
                json.dump(data, f)


if __name__ == '__main__':
    if not os.path.exists('data.json'):
        logging.error('Не найден файл data.json')
        exit()

    if not config.WILD_TOKEN:
        logging.error('Не установлен ключ WildBerries')
        exit()

    if config.TYPE not in ['LOGISTICS', 'FF']:
        logging.error('Некорректный режим работы')
        exit()

    logging.info(f'Режим работы: {config.TYPE}')
    if config.TYPE == 'LOGISTICS':
        if not config.CDEK_PUBLIC_KEY or not config.CDEK_SECRET_KEY:
            logging.error('Не установлены ключи СДЭК')
            exit()
        if not config.SEND_PVZ:
            logging.error('Не установлен ПВЗ отправки')
            exit()
    elif config.TYPE == 'FF':
        if not config.FF_PUBLIC_KEY or not config.FF_SECRET_KEY:
            logging.error('Не установлены ключи ФФ')
            exit()
        if not config.FF_SHOP:
            logging.error('Не установлен магазин ФФ')
            exit()
        if not config.FF_WAREHOUSE:
            logging.error('Не установлен склад ФФ')
            exit()
        if not config.FF_SENDER:
            logging.error('Не установлен отправитель ФФ')
            exit()

    main()
    logging.info('Скрипт завершил работу')

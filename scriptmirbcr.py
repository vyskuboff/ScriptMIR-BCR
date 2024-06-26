import pymysql
import psycopg2
import re
import math
import time
import logging
import requests
from datetime import datetime, timedelta
from config import TOKENTG_BOT,TOKENTG_BOT2, YOUR_CHAT_ID, STATIC_PHONE_NUMBER, PASSWORDDB, PASSWORDMYSQL, HOSTMYSQL, USERMYSQL, DBMYSQL

static_sender_phone_number = STATIC_PHONE_NUMBER
static_receiver_phone_number = STATIC_PHONE_NUMBER

# Параметры подключения к базам данных
mysql_params = {
    'host': HOSTMYSQL,
    'user': USERMYSQL,
    'password': PASSWORDMYSQL,
    'db': DBMYSQL,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

pg_params = {
    "host": "127.0.0.1",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": PASSWORDDB
}

# Токены ботов Telegram
TOKEN_TG_BOT = TOKENTG_BOT
TOKEN_TG_BOT2 = TOKENTG_BOT2

# Список отправителей от которых обрабатываем смс
allowed_senders = {'900', ...}

# Паттерны с возможностью модифицировать в будущем
# Регулярные выражения
operation_amount_pattern = re.compile(r'(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE)
balance_pattern = re.compile(r'(?:Баланс|Остаток)\s*[:\-]?\s*(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE)
mir_card_pattern = re.compile(r'(?:МИР|MIR|Карта|Karta)\s*\*?\s*-?\s*(\d{4})', re.IGNORECASE)
mir_account_pattern = re.compile(r'СЧ[ЕЁ]Т(\d{4})', re.IGNORECASE)
stop_words_patterns = [re.compile(r'недостаточно средств|отказ|nedostatochno sredstv|otkaz', re.IGNORECASE)]
one_time_code_patterns = [re.compile(r'код(?:\:)?\s*(\d+)', re.IGNORECASE)]
merchant_patterns = [
    re.compile(r'(?:Покупка|Оплата|Списание|Выдача|Отмена покупки|Отмена платежа|Оформлена отмена покупки)\s+\d[\d\s]*[\.,]?\d*\s*р\s+(.*?)\s+(?:Баланс|Остаток)', re.IGNORECASE),
    re.compile(r'Перевод\s+\d[\d\s]*[\.,]?\d*\s*р\s+от\s+(.*?)\s+(?:Баланс|Остаток)', re.IGNORECASE),
    re.compile(r'Перевод\s+из\s+(.*?)\s+(?:Баланс|Остаток)', re.IGNORECASE),
    re.compile(r'от\s+(.*?)\s+(?:Баланс|Остаток)', re.IGNORECASE),
    re.compile(r'по СБП\s+\d[\d\s]*[\.,]?\d*\s*р\s+(.*?)\s+(?:Баланс|Остаток)', re.IGNORECASE),
    re.compile(r'зачислен перевод по СБП\s+\d[\d\s]*[\.,]?\d*\s*р\s+(.*?)\s+(?:Баланс|Остаток)', re.IGNORECASE)
]
income_patterns = [
    re.compile(r'Перевод\s+из\s+.*?\+?(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE),
    re.compile(r'Перевод\s+(\d[\d\s]*[\.,]?\d*)\s*р\s+от', re.IGNORECASE),
    re.compile(r'(?:зачисление|пополнение)\s+(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE),
	re.compile(r'зачислен перевод по СБП\s+(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE)	
]
expense_patterns = [
    re.compile(r'(?:Покупка|Оплата|Списание|Выдача)\s+(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE),
    re.compile(r'перевод\s+(\d[\d\s]*[\.,]?\d*)\s*р(?!\s+от|\s+из)', re.IGNORECASE),
	re.compile(r'Покупка по СБП\s+(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE)
]
refund_patterns = [
    re.compile(r'Отмена покупки\s+(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE),
    re.compile(r'Отмена платежа\s+(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE),
    re.compile(r'Оформлена отмена покупки\s+(\d[\d\s]*[\.,]?\d*)\s*р', re.IGNORECASE)
]

# Функция для поиска совпадений по паттернам
def find_pattern(patterns, sms_text):
    for pattern in patterns:
        match = pattern.search(sms_text)
        if match:
            # Возвращаем содержимое первой группы захвата, если она есть, иначе возвращаем полное совпадение
            return match.group(1) if match.groups() else match.group(0)
    return None

# Функция для обработки SMS
def process_sms(sms_data, mysql_conn, pg_conn):
    sms_text = sms_data['message']
    sms_id = sms_data['id']
    sender = sms_data['sender']
    timestamp = sms_data['received_at']  # Предполагаем, что время получения SMS хранится в поле 'received_at'

    logging.info(f"Начало обработки SMS с ID: {sms_id}")

    if sender not in allowed_senders:
        send_telegram_message(f"SMS обработано:\n Отправителя {sender} нет в списке обрабатываемых - игнорируем смс\n{sms_text}")
        mark_sms_as_processed(sms_id, mysql_conn)
        return

    if find_pattern(stop_words_patterns, sms_text):
        send_telegram_message(f"SMS обработано:\n Найдено стоп слово (Отказ или недостаточно средств) - игнорируем смс\n{sms_text}")
        mark_sms_as_processed(sms_id, mysql_conn)
        return

    if "Ваш доход зарегистрирован в ФНС. Чек по услуге" in sms_text:
        send_telegram_message(f"SMS обработано:\n Найдено сообщение о доходе, зарегистрированном в ФНС - игнорируем смс")
        mark_sms_as_processed(sms_id, mysql_conn)
        return

    identifier = None
    phone_number = None
    is_mir_card = False
    mir_card_match = mir_card_pattern.search(sms_text)
    mir_account_match = mir_account_pattern.search(sms_text)
    
    if mir_card_match:
        identifier = mir_card_match.group(1) if mir_card_match else None
        phone_number = find_phone_by_mir_card(identifier, pg_conn) if identifier else None
        is_mir_card = True
    elif mir_account_match:
        identifier = mir_account_match.group(1) if mir_account_match else None
        phone_number = find_phone_by_account(identifier, pg_conn) if identifier else None

    logging.info(f"Идентификатор: {identifier}, Телефон: {phone_number}, Карта МИР: {is_mir_card}")

    if phone_number:
        if find_pattern(one_time_code_patterns, sms_text):
            handle_one_time_code(sms_text, sms_id, mysql_conn, pg_conn, phone_number)
            return

        balance_match = balance_pattern.search(sms_text)
        if balance_match:
            handle_balance(sms_text, phone_number, pg_conn)

        # Проверка на полное дублирование текста SMS
        if is_duplicate_sms(sms_text, mysql_conn):
            mark_sms_as_processed(sms_id, mysql_conn)
            send_telegram_message(f"SMS обработано:\n Игнорируем SMS, найден дубль\n{sms_text}")
            return
        
        merchant = find_pattern(merchant_patterns, sms_text)
        logging.info(f"Мерчант: {merchant}")

        if find_pattern(income_patterns, sms_text):
            logging.info(f"Обработка прихода для SMS с ID: {sms_id}")
            handle_income(sms_text, sms_id, mysql_conn, pg_conn, phone_number, identifier, is_mir_card, sender, timestamp, merchant)
            return

        if find_pattern(expense_patterns, sms_text):
            logging.info(f"Обработка расхода для SMS с ID: {sms_id}")
            handle_expense(sms_text, sms_id, mysql_conn, pg_conn, phone_number, identifier, is_mir_card, sender, timestamp, merchant)
            return

        if find_pattern(refund_patterns, sms_text):
            logging.info(f"Обработка возврата для SMS с ID: {sms_id}")
            handle_refund(sms_text, sms_id, mysql_conn, pg_conn, phone_number, identifier, is_mir_card, sender, timestamp, merchant)
            return

    # Если идентификатор найден, но телефон не найден в базе данных
    if identifier:
        if find_pattern(income_patterns, sms_text):
            if is_mir_card:
                send_telegram_message(f"SMS обработано:\n Найден приход по карте мир {identifier}, но не найден пользователь\n{sms_text}")
            else:
                send_telegram_message(f"SMS обработано:\n Найден приход по счету {identifier}, но не найден пользователь\n{sms_text}")
        elif find_pattern(expense_patterns, sms_text):
            if is_mir_card:
                send_telegram_message(f"SMS обработано:\n Найден расход по карте мир {identifier}, но не найден пользователь\n{sms_text}")
            else:
                send_telegram_message(f"SMS обработано:\n Найден расход по счету {identifier}, но не найден пользователь\n{sms_text}")
        elif find_pattern(refund_patterns, sms_text):
            if is_mir_card:
                send_telegram_message(f"SMS обработано:\n Найден возврат по карте мир {identifier}, но не найден пользователь\n{sms_text}")
            else:
                send_telegram_message(f"SMS обработано:\n Найден возврат по счету {identifier}, но не найден пользователь\n{sms_text}")
        else:
            if is_mir_card:
                send_telegram_message(f"SMS обработано:\n Найдена карта мир {identifier}, но не найдены данные для обработки (нет расхода/прихода)\n{sms_text}")
            else:
                send_telegram_message(f"SMS обработано:\n Найден счет {identifier}, но не найдены данные для обработки (нет расхода/прихода)\n{sms_text}")
    else:
        send_telegram_message(f"SMS обработано:\n В SMS не найдены данные для обработки\n{sms_text}")
    
    mark_sms_as_processed(sms_id, mysql_conn)

def handle_one_time_code(sms_text, sms_id, mysql_conn, pg_conn, phone_number):
    user_id = find_user_id_by_phone(phone_number, pg_conn)
    if user_id:
        one_time_code = find_pattern(one_time_code_patterns, sms_text)
        if one_time_code:
            send_one_time_code_to_user(one_time_code, user_id)
            mark_sms_as_processed(sms_id, mysql_conn)
            send_telegram_message(f"SMS обработано:\n Найден и направлен пользователю одноразовый код {one_time_code}\n{sms_text}")
            return True
    mark_sms_as_processed(sms_id, mysql_conn)
    send_telegram_message(f"SMS обработано:\n Найден одноразовый код в SMS, не найден пользователь\n{sms_text}")
    return False

def handle_balance(sms_text, phone_number, pg_conn):
    balance_match = balance_pattern.search(sms_text)
    if balance_match:
        balance = balance_match.group(1).replace(" ", "").replace("\xa0", "").replace(",", ".")
        with pg_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE users
                SET balance_mir_karta = %s
                WHERE phone_number = %s
            """, (balance, phone_number,))
            pg_conn.commit()

def handle_income(sms_text, sms_id, mysql_conn, pg_conn, phone_number, identifier, is_mir_card, sender, timestamp, merchant):
    try:
        user_info = find_info_by_phone(phone_number, pg_conn)
        amount_match = operation_amount_pattern.search(sms_text)
        balance_match = balance_pattern.search(sms_text)
        if amount_match:
            amount_str = amount_match.group(1).replace("\xa0", "").replace(" ", "").replace(",", ".")
            amount = float(amount_str)
            final_amount = math.floor(amount * 0.96)
            balance = None
            if balance_match:
                balance_str = balance_match.group(1).replace("\xa0", "").replace(" ", "").replace(",", ".")
                balance = float(balance_str)
            sender_phone_number = static_sender_phone_number
            sender_info = find_info_by_phone(sender_phone_number, pg_conn)
            if is_mir_card:
                comment = f'Автопополнение на {final_amount} BCR по карте мир {identifier}\n{sms_text}'
            else:
                comment = f'Автопополнение на {final_amount} BCR по счету {identifier}\n{sms_text}'

            # Проверка на дублирование транзакций
            if is_duplicate_transaction("приход", final_amount, identifier, is_mir_card, sender, merchant, mysql_conn):
                comment += "\nВнимание: транзакция возможно дублирующая, проверьте."
                send_telegram_message(f"SMS обработано:\n Найдена дублирующая транзакция по карте/счету {identifier} на сумму {amount} - обработка прекращена\n{sms_text}")
                mark_sms_as_processed(sms_id, mysql_conn)  # Помечаем SMS как обработанное
                return False  # Прекращаем обработку

            create_pending_action(sender_phone_number, phone_number, final_amount, sender_info, user_info, comment, pg_conn)
            mark_sms_as_processed(sms_id, mysql_conn)

            # Запись информации о транзакции в таблицу transaction_records
            with mysql_conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO transaction_records (sms_id, transaction_type, amount, balance, identifier, is_mir_card, sender, timestamp, merchant)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (sms_id, "приход", final_amount, balance, identifier, is_mir_card, sender, timestamp, merchant))
                mysql_conn.commit()

            send_telegram_message(f"SMS обработано:\n {comment}")
            return True
        else:
            logging.error(f"Не удалось извлечь данные из SMS с ID: {sms_id}")
            return False
    except Exception as e:
        logging.error(f"Ошибка при обработке прихода для SMS с ID: {sms_id}: {e}")
        return False

def handle_expense(sms_text, sms_id, mysql_conn, pg_conn, phone_number, identifier, is_mir_card, sender, timestamp, merchant):
    try:
        user_info = find_info_by_phone(phone_number, pg_conn)
        amount_match = operation_amount_pattern.search(sms_text)
        balance_match = balance_pattern.search(sms_text)
        if amount_match:
            amount_str = amount_match.group(1).replace("\xa0", "").replace(" ", "").replace(",", ".")
            amount = float(amount_str)
            final_amount = math.ceil(amount * 1.1)
            balance = None
            if balance_match:
                balance_str = balance_match.group(1).replace("\xa0", "").replace(" ", "").replace(",", ".")
                balance = float(balance_str)
            receiver_phone_number = static_receiver_phone_number
            receiver_info = find_info_by_phone(receiver_phone_number, pg_conn)
            if is_mir_card:
                comment = f'Автосписание на {final_amount} BCR по карте мир {identifier}\n{sms_text}'
            else:
                comment = f'Автосписание на {final_amount} BCR по счету {identifier}\n{sms_text}'

            # Проверка на дублирование транзакций
            if is_duplicate_transaction("расход", final_amount, identifier, is_mir_card, sender, merchant, mysql_conn):
                comment += "\nВнимание: транзакция возможно дублирующая, проверьте."
                send_telegram_message(f"SMS обработано:\n Найдена дублирующая транзакция по карте/счету {identifier} на сумму {amount} - обработка продолжена\n{sms_text}")

            create_pending_action(phone_number, receiver_phone_number, final_amount, user_info, receiver_info, comment, pg_conn)
            mark_sms_as_processed(sms_id, mysql_conn)

            # Запись информации о транзакции в таблицу transaction_records
            with mysql_conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO transaction_records (sms_id, transaction_type, amount, balance, identifier, is_mir_card, sender, timestamp, merchant)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (sms_id, "расход", final_amount, balance, identifier, is_mir_card, sender, timestamp, merchant))
                mysql_conn.commit()

            send_telegram_message(f"SMS обработано:\n {comment}")
            return True
        else:
            logging.error(f"Не удалось извлечь данные из SMS с ID: {sms_id}")
            return False
    except Exception as e:
        logging.error(f"Ошибка при обработке расхода для SMS с ID: {sms_id}: {e}")
        return False

def handle_refund(sms_text, sms_id, mysql_conn, pg_conn, phone_number, identifier, is_mir_card, sender, timestamp, merchant):
    try:
        user_info = find_info_by_phone(phone_number, pg_conn)
        amount_match = find_pattern(refund_patterns, sms_text)
        balance_match = balance_pattern.search(sms_text)
        if amount_match:
            amount_str = amount_match.replace("\xa0", "").replace(" ", "").replace(",", ".")
            amount = float(amount_str)
            final_amount = math.floor(amount * 0.96)
            balance = None
            if balance_match:
                balance_str = balance_match.group(1).replace("\xa0", "").replace(" ", "").replace(",", ".")
                balance = float(balance_str)
            sender_phone_number = static_sender_phone_number
            sender_info = find_info_by_phone(sender_phone_number, pg_conn)
            if is_mir_card:
                comment = f'Возврат на {final_amount} BCR по карте мир {identifier}\n{sms_text}'
            else:
                comment = f'Возврат на {final_amount} BCR по счету {identifier}\n{sms_text}'

            # Проверка на дублирование транзакций
            if is_duplicate_transaction(sms_id, "возврат", final_amount, balance, identifier, is_mir_card, sender, timestamp, merchant, mysql_conn):
                comment += "\nВнимание: транзакция возможно дублирующая, проверьте."
                send_telegram_message(f"SMS обработано:\n Найдена дублирующая транзакция по карте/счету {identifier} на сумму {amount} Игнорируем смс:\n{sms_text}")
                mark_sms_as_processed(sms_id, mysql_conn)  # Помечаем SMS как обработанное
                return False  # Прекращаем обработку

            create_pending_action(sender_phone_number, phone_number, final_amount, sender_info, user_info, comment, pg_conn)
            mark_sms_as_processed(sms_id, mysql_conn)

            # Запись информации о транзакции в таблицу transaction_records
            with mysql_conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO transaction_records (sms_id, transaction_type, amount, balance, identifier, is_mir_card, sender, timestamp, merchant)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (sms_id, "возврат", final_amount, balance, identifier, is_mir_card, sender, timestamp, merchant))
                mysql_conn.commit()

            send_telegram_message(f"SMS обработано:\n {comment}")
            return True
        else:
            logging.error(f"Не удалось извлечь данные из SMS с ID: {sms_id}")
            return False
    except Exception as e:
        logging.error(f"Ошибка при обработке возврата для SMS с ID: {sms_id}: {e}")
        return False
    
def send_one_time_code_to_user(one_time_code, user_id):
    # URL для отправки сообщения через Telegram Bot API, используя TOKENTG_BOT2
    send_message_url = f"https://api.telegram.org/bot{TOKENTG_BOT2}/sendMessage"
    
    # Формируем данные для отправки
    data = {
        'chat_id': user_id,  # Используем user_id как chat_id
        'text': f"Ваш одноразовый код: {one_time_code}"
    }
    
    # Отправляем POST-запрос на Telegram Bot API
    response = requests.post(send_message_url, data=data)

def send_telegram_message(message):
    url = "https://api.telegram.org/bot{}/sendMessage".format(TOKENTG_BOT)
    payload = {
        "chat_id": YOUR_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(url, data=payload)

def find_phone_by_mir_card(mir_card_number, pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT phone_number FROM users WHERE mir_karta LIKE %s", ('%' + mir_card_number,))
        result = cursor.fetchone()
        if result:
            return result[0]  # Возвращаем номер телефона
        else:
            return None
        
def find_phone_by_account(account_number, pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT phone_number FROM users WHERE mir_account LIKE %s", ('%' + account_number,))
        result = cursor.fetchone()
        if result:
            return result[0]  # Возвращаем номер телефона
        else:
            return None

def find_info_by_phone(phone_number, pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT info FROM users WHERE phone_number = %s", (phone_number,))
        result = cursor.fetchone()
        if result:
            return result[0]  # Возвращаем информацию о пользователе
        else:
            return "Информация не найдена"
        
def find_user_id_by_phone(phone_number, pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT user_id FROM assoc WHERE phone_number = %s", (phone_number,))
        result = cursor.fetchone()
        if result:
            return result[0]  # Возвращаем user_id для отправки кода
        else:
            return None

def create_pending_action(user_phone_number, receiver_phone_number, amount, sender_info, receiver_info, comment, pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO pending_actions (user_phone_number, receiver_phone_number, amount, sender_info, receiver_info, comment)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (user_phone_number, receiver_phone_number, amount, sender_info, receiver_info, comment))
        pg_conn.commit()

# Функция для обновления статуса SMS в базе данных MySQL
def mark_sms_as_processed(sms_id, mysql_conn):
    with mysql_conn.cursor() as cursor:
        cursor.execute("UPDATE sms_messages SET processed = TRUE WHERE id = %s", (sms_id,))
        mysql_conn.commit()

def connect_to_mysql(mysql_params, max_retries=3, delay=2):
    retries = 0
    while retries < max_retries:
        try:
            mysql_conn = pymysql.connect(**mysql_params)
            return mysql_conn
        except pymysql.err.OperationalError as e:
            retries += 1
            time.sleep(delay)
    raise Exception("Не удалось подключиться к MySQL после нескольких попыток")

def delete_old_processed_sms(mysql_conn):
    try:
        with mysql_conn.cursor() as cursor:
            # Удаляем обработанные SMS старше 60 минут
            cursor.execute("DELETE FROM sms_messages WHERE processed = TRUE AND received_at < NOW() - INTERVAL 60 MINUTE")
            mysql_conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при удалении старых обработанных SMS: {e}")

def delete_old_processed_transactions(mysql_conn):
    try:
        with mysql_conn.cursor() as cursor:
            # Удаляем записи старше 60 минут
            cursor.execute("DELETE FROM transaction_records WHERE timestamp < NOW() - INTERVAL 60 MINUTE")
            mysql_conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при удалении старых записей о транзакциях: {e}")        

def is_duplicate_sms(sms_text, mysql_conn):
    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM sms_messages WHERE message = %s AND processed = TRUE", (sms_text,))
            result = cursor.fetchone()
            return result['COUNT(*)'] > 0
    except Exception as e:
        logging.error(f"Ошибка при проверке на дублирование SMS: {e}")
        return False

# def is_duplicate_transaction(sms_id, transaction_type, amount, balance, identifier, is_mir_card, sender, timestamp, merchant, mysql_conn):
#     try:
#         logging.info(f"Проверка на дублирование транзакции для SMS с ID: {sms_id}, тип: {transaction_type}, сумма: {amount}, баланс: {balance}, идентификатор: {identifier}, карта МИР: {is_mir_card}, отправитель: {sender}, время: {timestamp}, мерчант: {merchant}")
#         with mysql_conn.cursor() as cursor:
#             cursor.execute("""
#                 SELECT COUNT(*) FROM transaction_records 
#                 WHERE sms_id != %s 
#                 AND transaction_type = %s 
#                 AND amount = %s 
#                 AND identifier = %s 
#                 AND is_mir_card = %s 
#                 AND sender = %s
#                 AND timestamp = %s
#                 AND merchant = %s
#                 AND timestamp > NOW() - INTERVAL 1 HOUR
#             """, (sms_id, transaction_type, amount, identifier, is_mir_card, sender, timestamp, merchant))
#             result = cursor.fetchone()
#             logging.info(f"Результат проверки на дублирование транзакции: {result['COUNT(*)']}")
#             return result['COUNT(*)'] > 0
#     except Exception as e:
#         logging.error(f"Ошибка при проверке на дублирование транзакции в MySQL: {e}")
#         return False

def is_duplicate_transaction(transaction_type, amount, identifier, is_mir_card, sender, merchant, mysql_conn):
    try:
        logging.info(f"Проверка на дублирование транзакции: тип: {transaction_type}, сумма: {amount}, идентификатор: {identifier}, карта МИР: {is_mir_card}, отправитель: {sender}, мерчант: {merchant}")
        with mysql_conn.cursor() as cursor:
            query = """
                SELECT COUNT(*) FROM transaction_records 
                WHERE transaction_type = %s 
                AND amount = %s 
                AND identifier = %s 
                AND is_mir_card = %s 
                AND sender = %s
                AND merchant = %s
                AND timestamp > NOW() - INTERVAL 1 HOUR
            """
            params = (transaction_type, amount, identifier, is_mir_card, sender, merchant)
            logging.info(f"SQL Query: {query}")
            logging.info(f"Query Parameters: {params}")
            cursor.execute(query, params)
            result = cursor.fetchone()
            logging.info(f"Результат проверки на дублирование транзакции: {result}")
            return result['COUNT(*)'] > 0
    except Exception as e:
        logging.error(f"Ошибка при проверке на дублирование транзакции в MySQL: {e}")
        return False

def main():
    error_notified = False

    # Создаем соединение с базой данных PostgreSQL
    pg_conn = psycopg2.connect(**pg_params)

    try:
        while True:
            mysql_conn = None
            try:
                # Создаем соединение с базой данных MySQL
                mysql_conn = connect_to_mysql(mysql_params)

                # Получаем необработанные SMS из базы данных MySQL
                with mysql_conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM sms_messages WHERE processed = FALSE")
                    unprocessed_sms = cursor.fetchall()

                # Обрабатываем каждое SMS
                for sms_data in unprocessed_sms:
                    process_sms(sms_data, mysql_conn, pg_conn)

                # Удаляем старые обработанные SMS
                delete_old_processed_sms(mysql_conn)

                # Удаляем старые записи о транзакциях
                delete_old_processed_transactions(mysql_conn)

                # Сбрасываем флаг ошибки при успешном соединении
                error_notified = False

            except Exception as e:
                if not error_notified:
                    send_telegram_message(f"Ошибка подключения к Reg.ru серверу СМС: \n{e} \n Бот продолжает попытки соединения - как только соединение будет восстановлено, будут обработаны все смс")
                    error_notified = True

            finally:
                # Закрываем соединение с базой данных MySQL, если оно было открыто
                if mysql_conn:
                    mysql_conn.close()

            # Пауза перед следующим циклом опроса
            time.sleep(5)  # Задержка в 5 секунд

    except KeyboardInterrupt:
        # Обработка исключения при прерывании программы (например, через Ctrl+C)
        print("Программа остановлена вручную")

    finally:
        # Закрываем соединение с базой данных PostgreSQL
        pg_conn.close()

# Основной цикл обработки
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()

import requests
import mysql.connector

class ParsingController:
    def __init__(self):
        self.base_url = 'https://gis.gosreestr.kz/'
        self.timeout = 30
        self.db_connection = mysql.connector.connect(
            host="localhost",
            user="nurbek",
            password="1",
            database="scraping"
        )
        self.cursor = self.db_connection.cursor()

    def parse_data(self, fl_id=None, fl_type_id='ATS'):
        last_id = self.get_last_parsed_id() or 1

        data = self.get_list_values(fl_id or last_id, fl_type_id)
        if not data:
            return

        for item in data:
            self.save_parsed_data(item, fl_id or last_id)
            self.parse_data(item['flId'], item['flTypeId'])

        self.save_last_parsed_id(fl_id or last_id)

    def get_list_values(self, fl_id, fl_type_id):
        try:
            response = requests.get(self.base_url + 'p/ru/address-registry/get-list-values', params={'flId': fl_id, 'flTypeId': fl_type_id}, timeout=self.timeout)
            response.raise_for_status()  # Проверка на ошибку HTTP
            data = response.json()  # Попытка декодирования JSON
            return data
        except requests.exceptions.RequestException as req_err:
            print(f'Request error occurred: {req_err}')  # Вывод ошибки запроса
            return None

    def save_parsed_data(self, item, parent_id):
        sql = "INSERT INTO parsed_data (flId, flTypeId, flText, flType, flSubType, flCato, flRca, parentId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (item.get('flId', ''), item.get('flTypeId', ''), item.get('flText', ''), item.get('flType', ''), item.get('flSubType', ''), item.get('flCato', ''), item.get('flRca', ''), parent_id)
        try:
            self.cursor.execute(sql, val)
            self.db_connection.commit()
        except mysql.connector.Error as db_err:
            print(f'Database error occurred: {db_err}')  # Вывод ошибки базы данных

    def get_last_parsed_id(self):
        try:
            sql = "SELECT flId FROM parsed_data ORDER BY id DESC LIMIT 1"
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            if result:
                return result[0]
        except mysql.connector.Error as db_err:
            print(f'Database error occurred: {db_err}')  # Вывод ошибки базы данных
        return None

    def save_last_parsed_id(self, fl_id):
        sql = "INSERT INTO parsed_data (flId) VALUES (%s)"
        val = (fl_id,)
        try:
            self.cursor.execute(sql, val)
            self.db_connection.commit()
        except mysql.connector.Error as db_err:
            print(f'Database error occurred: {db_err}')  # Вывод ошибки базы данных

# Пример использования
controller = ParsingController()
controller.parse_data()

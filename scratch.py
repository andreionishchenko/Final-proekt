import mysql.connector



# def get_connect():  # создание функции для чтения  из  базы данных
#     dbconfig = {
#         'host': 'ich-db.ccegls0svc9m.eu-central-1.rds.amazonaws.com',
#         'user': 'ich1',
#         'password': 'password',
#         'database': 'sakila'}
#
#     connection = mysql.connector.connect(**dbconfig)  # соединнение с б.д. 'sakila'
#     cursor = connection.cursor()  # создается курсор для отправки запросов и получения результатов
#     return cursor, connection


def get_connect_write():  # создание функции для записей в базу данных
    dbconfig = {
        'host': 'mysql.itcareerhub.de',
        'user': 'ich1',
        'password': 'ich1_password_ilovedbs',
        'database': '310524ptm_AO'}

    connection = mysql.connector.connect(**dbconfig)  # соединнение с bd 'sakila'
    cursor = connection.cursor()  
    return cursor, connection


# def database_exists(self):
#     self.cursor.execute("SHOW DATABASES LIKE %s", (self.database_name,))
#     return self.cursor.fetchone() is not None
#
#
# def create_database(self):
#     try:
#         self.cursor.execute(f"CREATE DATABASE {self.database_name}")
#         print(f"Database '{self.database_name}' created successfully.")
#     except mysql.connector.Error as err:
#         print(f"Failed to create database '{self.database_name}': {err}")
#
#
# def check_and_create_table(self):
#     table_exists = self.table_exists("history_search_sakila")
#     if not table_exists:
#         self.create_table()
#     else:
#         print("Table 'history_search_sakila' already exists.")
#
#
# def table_exists(self, table_name):
#     self.cursor.execute("SHOW TABLES LIKE %s", (table_name,))
#     return self.cursor.fetchone() is not None


def create_table(self, cursor=None):
    create_table_query = self.table_exists("history_search_sakila")
    sql = """ 
        CREATE TABLE history_search_sakila (
        id INT AUTO_INCREMENT PRIMARY KEY,
        query VARCHAR(100)
        )
        """
    try:
        self.cursor.execute(create_table_query)
        print("Table 'history_search_sakila' created successfully.")
    except mysql.connector.Error as e:
        print(f"Failed to create table 'history_search_sakila': {e}")

    cursor.close()  # По завершению работы с БД курсор и
    mysql.connector.close()

def save_query_to_db(cursor, connection, table_name, query):
    sql = f"INSERT INTO {table_name} (query) VALUES (%s);"
    cursor.execute(sql, (query,))
    connection.commit()

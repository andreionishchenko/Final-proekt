import mysql.connector
from scratch import get_connect_write, save_query_to_db


def get_connect():  # создание функции для чтения  из  базы данных
    dbconfig = {
        'host': 'ich-db.ccegls0svc9m.eu-central-1.rds.amazonaws.com',
        'user': 'ich1',
        'password': 'password',
        'database': 'sakila'}

    connection = mysql.connector.connect(**dbconfig)  # соединнение с б.д. 'sakila'
    cursor = connection.cursor()  # создается курсор для отправки запросов и получения результатов
    return cursor, connection


def get_connect_write():  # создание функции для записей в базу данных
    dbconfig = {
        'host': 'mysql.itcareerhub.de',
        'user': 'ich1',
        'password': 'ich1_password_ilovedbs',
        'database': '310524ptm_AO'}

    connection = mysql.connector.connect(**dbconfig)  # соединнение с bd 'sakila'
    cursor = connection.cursor()  # создается курсор для отправки запросов и получения результатов
    return cursor, connection


def get_search_by_keyword(cursor, keyword):
    sql = """                                           
        SELECT f.title, f.description
        FROM film f
        WHERE description LIKE %s 
        or title LIKE %s 
        or release_year LIKE %s                 # По ключевому слову находится 10 фильмов из таблици 'film'
        LIMIT 10;
        """
    cursor.execute(sql, ('%' + keyword + '%','%' + keyword + '%','%' + keyword + '%',))

    data = cursor.fetchall()  # cursor.fetchall  означает поиск всех записей
    for record in data:  # for record означает перебор всех данных в списке data
        print(record)  # который был получен через fetchall


def get_search_by_genre_and_yar(cursor, genre, year):
    sql = """                                           
        SELECT 
        f.title, f.release_year, c.name AS genre       #  поиск по жанру и году
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        WHERE c.name LIKE %s
        AND f.release_year = %s
        LIMIT 10;
        """
    cursor.execute(sql, ('%' + genre + '%', year,))

    data = cursor.fetchall()  # cursor.fetchall  означает поиск всех записей
    for record in data:  # for record означает перебор всех данных в списке data
        print(record)  # который был получен через fetchall


def get_search_by_top_10_pop(cursor, count):  # поиск 10 лучших запросов
    sql = """                                           
        SELECT query, COUNT(*) as cont
        FROM 310524ptm_AO.history_search_sakila
        GROUP BY query
        ORDER BY cont DESC
        limit %s;
        """
    cursor.execute(sql, (count,))

    data = cursor.fetchall()  # cursor.fetchall  означает поиск всех записей
    for record in data:  # for record означает перебор всех данных в списке data
        print(record)  # который был получен через fetchall


if __name__ == '__main__':

    cursor, connection = get_connect()
    cursor_write, connection_write = get_connect_write()

    while True:
        print("0 - завершить работу")
        print("1 - поиск по ключевому слову")
        print("2 - поиск по жанру и году")
        print("3 - поиск 10 самых популярных запросов")
        num_action = input("Введите число для выбора действия: ")
        if int(num_action) == 0:
            break

        elif int(num_action) == 1:
            keyword = input("Введите ключевое слово для поиска: ")
            save_query_to_db(cursor_write, connection_write, "history_search_sakila", keyword)
            get_search_by_keyword(cursor, keyword)

        elif int(num_action) == 2:
            while True:
                keyword = input("Введите  слово для поиска по жанру и число по году: ")
                if len(keyword.split()) == 2:
                    break
            genre = keyword.split()[0]
            year = keyword.split()[1]
            save_query_to_db(cursor_write, connection_write, "history_search_sakila", keyword)
            get_search_by_genre_and_yar(cursor, genre, year)

        elif int(num_action) == 3:
            # pass
            count = int(input("Введите количество самых популярных запросов: (стандарт 10): "))
            get_search_by_top_10_pop(cursor_write, count)

    cursor.close()  # По завершению работы с БД курсор и
    connection.close()  # соединение нужно закрывать

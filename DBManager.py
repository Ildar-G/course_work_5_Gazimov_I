import psycopg2

from utils import get_info, get_vacancies, delete_symbol

id_companies = [1740,     # яндекс
                3529,     # сбер
                1727451,  # транснефть-урал
                3776,     # МТС
                4181,     # ВТБ
                907345,   # лукойл
                1035394,  # Красное&белое
                39305,    # газпромнефть
                648780,   # башнефть-добыча
                10317521]  # Додо Пица


class DBManager:
    def __init__(self, db_name, params):
        self.db_name = db_name  # название базы данных
        self.params = params  # параметры подключение, получаем через config

    def connect_db(self):
        """Метод для подключения и создания БЗ"""

        connection = psycopg2.connect(database='postgres', **self.params)
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
            cursor.execute(f"CREATE DATABASE   {self.db_name}")
        except psycopg2.ProgrammingError:
            pass

        cursor.close()
        connection.close()

    def create_table(self):
        """Метод для создания таблиц с компаниями и вакансиями"""

        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cursor:
            try:
                cursor.execute("""
                           CREATE TABLE companies (
                           company_id int PRIMARY KEY,
                           company_name varchar(50) NOT NULL,
                           description text
                           )
                           """)

                cursor.execute("""
                            CREATE TABLE vacancies (
                            id_vacancies int PRIMARY KEY,
                            company_id int REFERENCES companies(company_id) NOT NULL,
                            vacancies_name varchar(100) NOT NULL,
                            url varchar(100) NOT NULL,
                            salary_from varchar(100), 
                            salary_to varchar(100),
                            salary_avr varchar(100),
                            salary_max varchar(100),
                            aria varchar(100)
                            )
                            """)

            except psycopg2.ProgrammingError:
                print("Таблицы не созданы")
        connection.commit()
        connection.close()

    def write_info_in_table(self):
        """Метод для заполнения информацией таблиц компаний и вакансий.
              Используются функции get_info, get_vacancies"""

        connection = psycopg2.connect(database=self.db_name, **self.params)
        hh = get_info(id_companies)
        with connection.cursor() as cursor:
            for i in range(len(hh)):
                hh_replace = delete_symbol(hh[i]['description'])  # удаляем ненужные символы из текста
                hh_gv = get_vacancies(hh[i]['url'])
                cursor.execute("""INSERT INTO companies VALUES (%s,%s,%s)""",
                               (hh[i]['company_id'], hh[i]['company_name'], hh_replace))
                for count in range(len(hh_gv)):
                    cursor.execute("""INSERT INTO vacancies VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s )""",
                                   (hh_gv[count]['id_vacancies'], hh_gv[count]['id_company'],
                                    hh_gv[count]['name'], hh_gv[count]['url'],
                                    hh_gv[count]['salary_from'], hh_gv[count]['salary_to'],
                                    hh_gv[count]['salary_avr'], hh_gv[count]['salary_max'],
                                    hh_gv[count]['area'],
                                    ))

        connection.commit()
        connection.close()

    def get_companies_and_vacancies_count(self):
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute("""SELECT company_name, COUNT(vacancies_name) AS count_vacancies 
                                FROM companies
                                JOIN vacancies USING (company_id) 
                                GROUP BY companies.company_id """)
            rows = cursor.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()

    def get_all_vacancies(self):
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute("""SELECT companies.company_name, vacancies.vacancies_name, 
                                vacancies.salary_avr, vacancies.url 
                                FROM companies
                                JOIN vacancies USING (company_id) """)
            rows = cursor.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()

    def get_vacancies_with_higher_salary(self):
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute("""SELECT * from vacancies
                              WHERE salary_max > salary_avr
                               ORDER BY salary_max """)
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        connection.commit()
        connection.close()

    def avr_salary(self):
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute("""SELECT vacancies_name,salary_avr
                                from vacancies 
                                ORDER BY salary_avr DESC """)
            rows = cursor.fetchall()
            for row in rows:
                print(row)
            connection.commit()
            connection.close()

    def get_vacancies_with_keyword(self, word):
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute(f""" SELECT * from vacancies
                              WHERE vacancies_name like '%{word}'
                              OR vacancies_name like '{word}%'
                              OR vacancies_name like '%{word}%' """)
            rows = cursor.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()

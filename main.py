from confing import config
from DBManager import DBManager

params = config()
db_manager = DBManager('Head_Hunter', params)
db_manager.connect_db()
db_manager.create_table()
db_manager.write_info_in_table()
db_manager.get_companies_and_vacancies_count()
db_manager.get_all_vacancies()
db_manager.get_vacancies_with_higher_salary()
db_manager.avr_salary()
db_manager.get_vacancies_with_keyword('word')

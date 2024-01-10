import requests  # Для запросов по API


def get_info(id_companies):
    """Получаем информацию о компаниях и формируем ее в словарь"""
    company_vacancies = []
    try:
        for id_company in id_companies:
            url_hh = f'https://api.hh.ru/employers/{id_company}'
            info = requests.get(url_hh).json()
            company_vacancies.append({'company_id': info['id'],
                                      'company_name': info['name'],
                                      'description': info['description'],
                                      'url': info['vacancies_url']
                                      })
    except KeyError:
        print("По данным критериям не нашлось вакансий")
    return company_vacancies


def get_vacancies(url):
    """Получаем информацию о вакансиях, используя url от компании.
            Полученную информацию формируем в словарь"""
    info_vacancies = []
    info = requests.get(url).json()['items']
    salary_max = 0
    try:
        for vacancies in info:
            if vacancies['salary']:
                salary_from = vacancies['salary']['from'] if vacancies['salary']['from'] else 0
                salary_to = vacancies['salary']['to'] if vacancies['salary']['to'] else 0
                salary_avr = (salary_from + salary_to) / 2
                if salary_from > salary_max:
                    salary_max = salary_from
                elif salary_to > salary_max:
                    salary_max = salary_to
            else:
                salary_from = 0
                salary_to = 0
                salary_avr = 0
                salary_max = 0
            info_vacancies.append({'id_vacancies': vacancies['id'],
                                   'id_company': vacancies['employer']['id'],
                                   'name': vacancies['name'],
                                   'url': vacancies['area']['url'],
                                   'salary_from': salary_from,
                                   'salary_to': salary_to,
                                   'salary_avr': salary_avr,
                                   'salary_max': salary_max,
                                   'area': vacancies['area']['name']
                                   })
    except KeyError:
        print("Неправильные критерии поиска")
    return info_vacancies


def delete_symbol(text):
    """Метод для удаления ненужных символов в тексте"""
    symbols = ['\n', '<strong>', '</strong>', '</p>', '<p>',
               '<b>', '</b>', '<ul>', '<br />', '</ul>', '&nbsp', '</li>', '</ul>',
               '&laquo', '&ndash', '&mdash', '<em>', '&middot', '</em>', '&raquo']
    for symbol in symbols:
        text = text.replace(symbol, '')
    return text

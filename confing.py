from configparser import ConfigParser


def config(filename="database.ini", section="postgresql"):
    confing = ConfigParser()
    confing.read(filename)
    db = {}
    if confing.has_section(section):
        params = confing.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Параметр не найден')
    return db

"""


"""
__author__ = "_FEAR_MOV_"
__version__ = 0.1
# Импортируемые библиотеки----------------------------------------------------------------------------------------------
import json
import sys
# import daemon
# import time
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql


# Функции---------------------------------------------------------------------------------------------------------------
def parser_config(*argvs):
    """
    Парсим конфигурационный файл.

    :param argvs: принимает аргументы из командной строки
    нужно для поиска -c, после которой указвается путь к конфигурационному файлу

    :return: возвращает словарь с конфигом
    """
    index = 0
    if "-c" in argvs[0]:
        index = argvs[0].index("-c") + 1
    with open("config.json" if index == 0 else argvs[0][index]) as inf:
        config_json = json.load(inf)
    return config_json


# Тело программы--------------------------------------------------------------------------------------------------------
config = parser_config(sys.argv)
print(config)
data_connect = config["relation_db"]["type_db"] + "://" + config["relation_db"]["user"] + ":" \
    + config["relation_db"]["password"] + "@" + config["relation_db"]["host"] + ":" + config["relation_db"]["port"] \
    + "/" + config["relation_db"]["dbname"]
engine = create_engine(data_connect)
connection = engine.connect()
s = connection.execute('SELECT * FROM naumb_file_type')
print(s.compile(dialect=postgresql.dialect()))

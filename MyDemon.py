"""


"""
__author__ = "_FEAR_MOV_"
# Импортируемые библиотеки----------------------------------------------------------------------------------------------
import psycopg2
from psycopg2.extras import DictCursor  # Для получения ответа из бд в виде словаря
import redis
import sys
import json
import consul
import daemon
import time


# Функции---------------------------------------------------------------------------------------------------------------
'''def data_conn(type_db):
    """
    Преобразуем аргументы консоли в словарь.

    :param type_db: строковая переменная, обозначающая тип БД (redis/psql), для которой составляется словарь.
    :return: возвращает словарь.
    """
    data = {}
    for argv in sys.argv:
        argv = argv.split("=")
        if type_db == "psql" and argv[0] == type_db:
            params = argv[1].split(":")
            data["dbname"] = params[0]
            data["host"] = params[1]
            data["user"] = params[2]
            data["password"] = params[3]
        elif type_db == "redis" and argv[0] == type_db:
            params = argv[1].split(":")
            data["host"] = params[0]
            data["port"] = params[1]
            data["db"] = params[2]
        elif type_db == "consul" and argv[0] == type_db:
            params = argv[1].split(":")
            data["host"] = params[0]
            data["port"] = params[1]
            data["dc"] = params[2]
    return data'''


def parser_config():
    pass


def main():
    global k

    # Переменная для формирования JSON файла
    dataJson = {}

    # Создаём словари для подключенний к PostgreSQL, Redis, Consul
    dataConnPostgreSQL = data_conn("psql")
    dataConnRedis = data_conn("redis")
    dataConnConsul = data_conn("consul")

    # Подключаемся к PostgreSQL и делаем запросы
    with psycopg2.connect(
            dbname=dataConnPostgreSQL["dbname"], host=dataConnPostgreSQL["host"],
            user=dataConnPostgreSQL["user"], password=dataConnPostgreSQL["password"]
    ) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute('SELECT * FROM naumb_version')
            dataJson["naumb_version"] = []
            naumb_version = {}
            i = 0
            for row in cursor:
                data = {}
                data["repository_id"] = row[0]
                data["repository_path"] = row[1]
                data["version"] = row[2]
                i += 1
                key = "naumb_version" + str(i)
                naumb_version[key] = data
                print(row)
            dataJson["naumb_version"].append(naumb_version)

            cursor.execute('SELECT * FROM naumb_file_type')
            naumb_file_type = {}
            for row in cursor:
                print(row)
                naumb_file_type[row[0]] = row[1]
            dataJson["naumb_file_type"] = naumb_file_type

            cursor.execute("SELECT count(*) FROM call_legs")
            kolvo_call_legs = (cursor.fetchone())
            dataJson["kolvo_call_legs"] = kolvo_call_legs[0]

    # Подключаемся к Redis и делаем запросы
    connRedis = redis.StrictRedis(host=dataConnRedis["host"], port=dataConnRedis["port"], db=dataConnRedis["db"])

    g = connRedis.smembers("online_agents")
    print(g)

    online_agents = connRedis.hgetall("online_agents:substate:normal")
    new_online_agents = {}
    for item in online_agents.items():
        key, value = map(str, item)
        new_online_agents[key] = value
    dataJson["online_agents:substate:normal"] = new_online_agents

    # Создаём json
    dataJson["param"] = k
    my_json = json.dumps(dataJson, indent=4)

    # Закидывем json в consul
    c = consul.Consul(host=dataConnConsul["host"], port=dataConnConsul["port"], dc=dataConnConsul["dc"])
    print(c.kv.put("log", my_json))


# Тело программы--------------------------------------------------------------------------------------------------------
# Техническая переменная для отслеживания количества перезаписей
k = 0

if "-d" in sys.argv:
    with daemon.DaemonContext():
        while True:
            k += 1
            main()
            time.sleep(30)
else:
    main()

# Альфа версия (Не смотреть)--------------------------------------------------------------------------------------------
# Расчетное время ожидания (EWT) по каждой очереди
# project_ids = connRedis.get("project_config:projects_set")
# for project_id in project_ids:
    # s = connRedis.get("project_config:%s:mean_wait" % project_id)
    # print(s)



# Расчёт уровня сервиса SL
"""with psycopg2.connect(
    dbname=dataConnPostrgeSQL["dbname"], host=dataConnPostrgeSQL["host"],
    user=dataConnPostrgeSQL["user"], password=dataConnPostrgeSQL["password"]
) as conn:
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        x = cursor.execute(
                           "SELECT project_id, count(session_id) \
                           FROM ns_inbound_call_data \
                           WHERE is_processed=True and is_processed_after_threshold=False \
                            GROUP BY project_id"
        )
        y = cursor.execute(
                            "SELECT project_id, count(session_id) \
                            FROM ns_inbound_call_data \
                            WHERE is_unblocked=True and is_shortly_abandoned=False \
                            GROUP BY project_id"
        )"""
        # print(str(int(x)/int(y)))


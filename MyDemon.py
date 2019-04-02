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
# import daemon
# import time


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


def main():
    """
    Собираем json файл, беря данные из Redis и PostgreSQL, и кладём в Consul

    :return: Ничего не возвращает
    """
    global k, config

    # Переменная для формирования JSON файла
    data_json = {}

    # Подключаемся к PostgreSQL и делаем запросы
    with psycopg2.connect(
            dbname=config["postgresql"]["dbname"], host=config["postgresql"]["host"],
            user=config["postgresql"]["user"], password=config["postgresql"]["password"]
    ) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute('SELECT * FROM naumb_version')
            data_json["naumb_version"] = []
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
            data_json["naumb_version"].append(naumb_version)

            cursor.execute('SELECT * FROM naumb_file_type')
            naumb_file_type = {}
            for row in cursor:
                print(row)
                naumb_file_type[row[0]] = row[1]
            data_json["naumb_file_type"] = naumb_file_type

            cursor.execute("SELECT count(*) FROM call_legs")
            kolvo_call_legs = (cursor.fetchone())
            data_json["kolvo_call_legs"] = kolvo_call_legs[0]

    # Подключаемся к Redis и делаем запросы
    conn_redis = redis.StrictRedis(
        host=config["redis"]["host"],
        port=config["redis"]["port"],
        db=config["redis"]["db"]
    )

    g = conn_redis.smembers("online_agents")
    print(g)

    online_agents = conn_redis.hgetall("online_agents:substate:normal")
    new_online_agents = {}
    for item in online_agents.items():
        key, value = map(str, item)
        new_online_agents[key] = value
    data_json["online_agents:substate:normal"] = new_online_agents

    # Создаём json
    data_json["param"] = k
    my_json = json.dumps(data_json, indent=4)

    # Закидывем json в consul
    c = consul.Consul(
        host=config["consul"]["host"],
        port=config["consul"]["port"],
        dc=config["consul"]["dc"]
    )
    print(c.kv.put("log", my_json))


# Тело программы--------------------------------------------------------------------------------------------------------
# Создаём словари для подключенний к PostgreSQL, Redis, Consul
config = parser_config(sys.argv)

# Техническая переменная для отслеживания количества перезаписей
k = 0
"""
if config[daemon]:
    with daemon.DaemonContext():
        while True:
            k += 1
            main()
            time.sleep(30)
else:"""
main()

# Альфа версия (Не смотреть)--------------------------------------------------------------------------------------------
"""
# Расчетное время ожидания (EWT) по каждой очереди
project_ids = connRedis.get("project_config:projects_set")
for project_id in project_ids:
    s = connRedis.get("project_config:%s:mean_wait" % project_id)
    print(s)



# Расчёт уровня сервиса SL
with psycopg2.connect(
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
        )
        print(str(int(x)/int(y)))
"""

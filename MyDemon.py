"""


"""
__author__ = "_FEAR_MOV_"
__version__ = 0.6
# Импортируемые библиотеки----------------------------------------------------------------------------------------------
import psycopg2
from psycopg2.extras import DictCursor  # Для получения ответа из бд в виде словаря
import redis
import sys
import json
import consul
# import daemon
# import time
import platform
import logging


# Функции---------------------------------------------------------------------------------------------------------------
def parser_config(*argvs):
    """
    Парсим конфигурационный файл.

    :param argvs: принимает аргументы из командной строки
    нужно для поиска -c, после которой указвается путь к конфигурационному файлу

    :return: возвращает словарь с конфигом
    """
    index = 0
    if "-c" in argvs:
        index = argvs.index("-c") + 1
    with open("config.json" if index == 0 else argvs[index]) as inf:
        config_json = json.load(inf)
    return config_json


def decode_bytes_in_str(x):
    x = str(x)
    return x[2:len(x) - 1]


def main():
    """
    Собираем json файл, беря данные из Redis и PostgreSQL, и кладём в Consul

    :return: Ничего не возвращает
    """
    global k, config

    logging.basicConfig(**config["logging"])

    logging.debug("This is a debug message")
    logging.info("Informational message")
    logging.error("An error has happened!")

    log = logging.getLogger("ex")

    try:
        raise RuntimeError
    except RuntimeError:
        log.error("Error!")
    # cursor_factory=DictCursor
    # Подключаемся к PostgreSQL и делаем запросы
    with psycopg2.connect(**config["postgresql"]) as conn:
        with conn.cursor() as cursor:
            # Расчёт уровня сервиса SL
            cursor.execute(
                "SELECT project_id, count(session_id) \
                FROM ns_inbound_call_data \
                WHERE is_processed=True and is_processed_after_threshold=False \
                GROUP BY project_id"
            )
            timed_answering_calls = {key: value for key, value in cursor}
            for row in cursor:
                print(row)

            cursor.execute(
                "SELECT project_id, count(session_id) \
                FROM ns_inbound_call_data \
                WHERE is_unblocked=True and is_shortly_abandoned=False \
                GROUP BY project_id"
            )
            queued_calls_without_irrelevant_missed_calls = {key: value for key, value in cursor}
            sl_projects = {key: value / queued_calls_without_irrelevant_missed_calls[key] for key, value in
                           timed_answering_calls.items()}
            del timed_answering_calls
            del queued_calls_without_irrelevant_missed_calls

    # Подключаемся к Redis и делаем запросы
    conn_redis = redis.StrictRedis(**config["redis"])

    # Расчетное время ожидания (EWT) по каждой очереди
    project_ids = conn_redis.smembers("project_config:projects_set")  # Запрашиваем id проктов
    ewt = {}
    print(project_ids)
    for project_id in project_ids:
        project_id = decode_bytes_in_str(project_id)
        print(project_id)
        ewt[project_id] = {}
        s = str(conn_redis.get("project_config:%s:mean_wait" % project_id))
        s = decode_bytes_in_str(s)
        ewt[project_id] = s
        print(ewt)

    # Переменная для формирования JSON файла
    data_json = {}
    # Заполняем переменную
    data_json["sl_instalation"] = sum(sl_projects.values())
    data_json["projects"] = []
    for key, value in sl_projects.items():
        data_json["projects"] += [{key: [{"el": value, "ewt": ewt[key]}]}]

    # Создаём json
    data_json["param"] = k
    my_json = json.dumps(data_json, indent=4)

    # Закидывем json в consul
    c = consul.Consul(**config["consul"])
    key = "Balancer/" + platform.node()
    print(c.kv.put(key, my_json))


# Тело программы--------------------------------------------------------------------------------------------------------
# Создаём словарь с конфигом
config = parser_config(*sys.argv)
# Техническая переменная для отслеживания количества перезаписей
k = 0
"""
if config["daemon"]:
    with daemon.DaemonContext():
        while True:
            k += 1
            main()
            time.sleep(config["timer"])
else:"""
main()

# Альфа версия (Не смотреть)--------------------------------------------------------------------------------------------

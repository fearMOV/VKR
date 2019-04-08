"""


"""
__author__ = "_FEAR_MOV_"
__version__ = 0.7
# Импортируемые библиотеки----------------------------------------------------------------------------------------------
import psycopg2
import redis
import sys
import json
import consul
# import daemon
# import time
import platform
import logging
import argparse


# Функции---------------------------------------------------------------------------------------------------------------
def decode_bytes_in_str(x):
    x = str(x)
    return x[2:len(x) - 1]


def main():
    """
    Собираем json файл, беря данные из Redis и PostgreSQL, и кладём в Consul

    :return: Ничего не возвращает
    """
    global k, config

    # logging.error("An error has happened!")

    # log = logging.getLogger("ex")

    # try:
    #     raise RuntimeError
    # except RuntimeError:
    #     log.error("Error!")

    # Подключаемся к PostgreSQL и делаем запросы
    with psycopg2.connect(**config["postgresql"]) as conn:
        conn.set_client_encoding('UTF8')
        with conn.cursor() as cursor:
            # Расчёт уровня сервиса SL
            cursor.execute(
                "SELECT project_id, count(session_id) \
                FROM ns_inbound_call_data \
                WHERE is_processed=True and is_processed_after_threshold=False \
                GROUP BY project_id"
            )
            timed_answering_calls = {key: value for key, value in cursor}
            logger.debug("Cвоевременно отвеченные вызовы: {}".format(timed_answering_calls))

            cursor.execute(
                "SELECT project_id, count(session_id) \
                FROM ns_inbound_call_data \
                WHERE is_unblocked=True and is_shortly_abandoned=False \
                GROUP BY project_id"
            )
            queued_calls_without_irrelevant_missed_calls = {key: value for key, value in cursor}
            logger.debug("Направленные в очередь вызовы за вычетом значения показателя \
Неактуальные пропущенные вызовы: {}".format(queued_calls_without_irrelevant_missed_calls))
            sl_projects = {key: value / queued_calls_without_irrelevant_missed_calls[key] for key, value in
                           timed_answering_calls.items()}
            logger.debug("Project_id: sl - {}".format(sl_projects))
            del timed_answering_calls
            del queued_calls_without_irrelevant_missed_calls

    # Подключаемся к Redis и делаем запросы
    conn_redis = redis.StrictRedis(**config["redis"])

    # Расчетное время ожидания (EWT) по каждой очереди
    project_ids = conn_redis.smembers("project_config:projects_set")  # Запрашиваем id проктов
    ewt = {}
    logger.debug("ID проектов из redis: {}".format(project_ids))
    for project_id in project_ids:
        project_id = decode_bytes_in_str(project_id)
        s = conn_redis.get("project_config:%s:mean_wait" % project_id)
        s = decode_bytes_in_str(s)
        ewt[project_id] = s
    logger.debug("EWT: {}".format(ewt))

    # Переменная для формирования JSON файла
    data_json = dict()
    # Заполняем переменную
    data_json["sl_instalation"] = sum(sl_projects.values())
    data_json["projects"] = []
    for key, value in sl_projects.items():
        data_json["projects"] += [{key: [{"el": value, "ewt": ewt[key]}]}]
    logger.debug("Файл json: {}".format(data_json))

    # Создаём json
    data_json["param"] = k
    my_json = json.dumps(data_json, indent=4)

    # Закидывем json в consul
    c = consul.Consul(**config["consul"])
    key = "Balancer/" + platform.node()
    print(c.kv.put(key, my_json))


# Тело программы--------------------------------------------------------------------------------------------------------
# Настройка логирования
logging.basicConfig(filename="sample.log", level=10)
logging.Formatter()
if __name__ != "__main__":
    logger = logging.getLogger(__name__)
else:
    logger = logging.getLogger("root")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.CRITICAL)
logger.addHandler(handler)

# Парсим конфигурационный файл
try:
    index = 0
    parser = argparse.ArgumentParser()
    parser.add_argument("-cf", "--config_file", nargs="?", const="config.json", default="config.json",
                        help="Опция для подключения конфигурационного файла, лежащего вне папки со скриптом")
    with open(parser.parse_args().config_file) as inf:
        config = json.load(inf)
except FileNotFoundError:
    logger.critical("Конфигурационный файл не найден")
    sys.exit(1)
except Exception as ex:
    print(sys.exc_info())
    sys.exit(2)
logger.debug("Конфигурационный файл: {}".format(config))


# Техническая переменная для отслеживания количества перезаписей
k = 0
"""
if config["daemon"]:
    with daemon.DaemonContext():
        while True:
            k += 1
            main()
            logging.debug("Сон {}".format(config["timer"]))
            time.sleep(config["timer"])
else:"""
main()

# Альфа версия (Не смотреть)--------------------------------------------------------------------------------------------

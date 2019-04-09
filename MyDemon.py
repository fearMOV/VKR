"""
Программа собирает метрики из Redis и PostgreSQL, и выкладываем в Consul в формате json
"""
__author__ = "_FEAR_MOV_"
__version__ = 0.7
# Импортируемые библиотеки----------------------------------------------------------------------------------------------

import psycopg2  # Для работы с PostgreSQL
import redis  # Для работы с Redis
import sys  # Для работы с переменными орукжения интерпритатора python
import json  # Для работы с форматом json
import consul  # Для работы с консул
# import daemon  # Для создания демона
# import time  # Для таймаута
import platform  # Для нахождения названия машины
import logging  # Для логирования
import argparse  # Для получения аргументов из консоли, переданных при запуске


# Функции---------------------------------------------------------------------------------------------------------------
def decode_bytes_in_str(x):
    """
    Функция декодинарования бинарных данных в строку.

    :param x: бинарная переменная из Redis
    :return: строковая переменная без b''
    """
    x = str(x)  # Преобразуем в строку бинарные данные
    return x[2:len(x) - 1]  # Обрезаем в начале строки b' и в конце строки '


def main():
    """
    Собираем json файл, беря данные из Redis и PostgreSQL, и выкладываем в Consul

    :return: Ничего не возвращает
    """
    global k, config  # Добавляем переменные в пространство имён функции из глобалного пространства имён

    # Подключаемся к PostgreSQL и делаем запросы
    with psycopg2.connect(**config["postgresql"]) as conn:  # Подключаемся к PostgreSQL
        logger.info("Подключение к БД PostgreSQL успешно")  # Логируем успешное подключение к PostgreSQL
        conn.set_client_encoding('UTF8')  # Деодируем получаемые данные в UNICODE
        with conn.cursor() as cursor:  # Открываем курсор для обращений к БД
            # Расчёт уровня сервиса SL
            # Запрашиваем значение показателя 'Cвоевременно отвеченные вызовы'
            """
            cursor.execute(
                "SELECT project_id, count(session_id) \
                FROM ns_inbound_call_data \
                WHERE is_processed=True and is_processed_after_threshold=False \
                GROUP BY project_id"
            )
            # Полученные данные преобразуем в словарь типа "project_id": "Количество_своевременно_отвеченных_вызовов"
            timed_answering_calls = {key: value for key, value in cursor}
            logger.debug("Cвоевременно отвеченные вызовы: {}".format(timed_answering_calls))  # Логируем словарь

            # Запрашиваем значение показателя Направленные в очередь вызовы за вычетом значения показателя
            # Неактуальные пропущенные вызовы
            cursor.execute(
                "SELECT project_id, count(session_id) \
                FROM ns_inbound_call_data \
                WHERE is_unblocked=True and is_shortly_abandoned=False \
                GROUP BY project_id"
            )
            # Полученыне даныне преобрауем в словарь типа "project_id": "Количество_Направленных_в очередь_вызовы_за
            # вычетом значения показателя_Неактуальные_пропущенные_вызовы"
            queued_calls_without_irrelevant_missed_calls = {key: value for key, value in cursor}
            logger.debug("Направленные в очередь вызовы за вычетом значения показателя \
Неактуальные пропущенные вызовы: {}".format(queued_calls_without_irrelevant_missed_calls))  # Логируем словарь
            sl_projects = {key: value / queued_calls_without_irrelevant_missed_calls[key] for key, value in
                           timed_answering_calls.items()}  # Высчитываем sl и записываем в словарь"""
            cursor.execute("""
                with timed_answering_calls as (
	                select project_id as id_project, cast(count(session_id) as dec) as tac
	                from ns_inbound_call_data
	                WHERE is_processed=True and is_processed_after_threshold=false
	                group by id_project
                )
                SELECT project_id, tac/count(session_id) as sl
                FROM ns_inbound_call_data, timed_answering_calls
                WHERE is_unblocked=True and is_shortly_abandoned=false
                group by project_id, tac"""
            )
            sl_projects = {key: float(value) for key, value in cursor}
            print(sl_projects)
            logger.debug("Project_id: sl - {}".format(sl_projects))  # Логируем полученный словарь
            # Удаляем не нужные словари
            # del timed_answering_calls
            # del queued_calls_without_irrelevant_missed_calls
        logger.info("Запросы к PostgreSQL выполнены")  # Логируем успешное выполнение запросов

    # Подключаемся к Redis и делаем запросы
    conn_redis = redis.StrictRedis(**config["redis"])  # Подключаемся к БД Redis
    logger.info("Подключение к БД Redis успешно")  # Логируем успешное подключение к БД Redis

    # Расчетное время ожидания (EWT) по каждой очереди
    project_ids = conn_redis.smembers("project_config:projects_set")  # Запрашиваем id проктов
    ewt = {}  # Создаём словарь для хранения информации полученной из Redis
    logger.debug("ID проектов из redis: {}".format(project_ids))  # Логируем полученыне id проектов
    for project_id in project_ids:  # Перебираем все полученные id проектов по одному
        project_id = decode_bytes_in_str(project_id)  # Преобразуем id проекта в строку
        s = conn_redis.get("project_config:%s:mean_wait" % project_id)  # Делаем запрос к Redis, для получения ewt
        s = decode_bytes_in_str(s)  # Преобразуем полученные даныне в строку
        ewt[project_id] = s  # Записваем в словарь типа "project_id": "ewt"
    logger.debug("EWT: {}".format(ewt))  # Логируем полученынй словарь

    # Переменная для формирования JSON файла
    data_json = dict()
    # Заполняем переменную
    data_json["sl_instalation"] = sum(sl_projects.values())  # Высчитываем SL инсталяции и записываем
    data_json["projects"] = []  # Будем создавать список проектов
    for key, value in sl_projects.items():  # Перебираем словарь с помощью items() получаем отдельно ключ и значение
        data_json["projects"].append([{key: [{"el": value, "ewt": ewt[key]}]}])  # Создаём словарь
    logger.debug("Файл json: {}".format(data_json))  # Логируем созданынй словарь

    # Создаём json
    data_json["param"] = k  # Техническая переменная
    my_json = json.dumps(data_json, indent=4)  # Преобразовываем словарь в json

    # Закидывем json в consul
    c = consul.Consul(**config["consul"])  # Подключаемся к Consul
    key = "Balancer/" + platform.node()  # Создаём переменную ключа для передачи в Consul
    print(c.kv.put(key, my_json))  # Передаём сгенерированный json фалй по заданному ключу в Consul


# Тело программы--------------------------------------------------------------------------------------------------------
# Парсим аргументы из консоли, переданные при запуске скрипта
parser = argparse.ArgumentParser()  # Создаём объект класса argparse
parser.add_argument(  # Добавляем аргумент
    "-cf",  # -cf получаемый из консоли
    "--config_file",  # в программе будет переменная config_file
    nargs="?",  # аргумента может не быть, или есть но не передано значение
    const="config.json",  # если аргумент есть, но нет значения, то передаётся config.json
    default="config.json",  # если аргумента нет, то передаём как настройку по умолчанию config.json
    help="Опция для подключения конфигурационного файла, лежащего вне папки со скриптом"  # выводится при -h
)
parser.add_argument(
    "-lf",
    "--log_file",
    nargs="?",
    const="sample.log",
    default="sample.log",
    help="Опция для создания лог файла не по умолчанию"
)
parser.add_argument(
    "-ll",
    "--log_level",
    nargs="?",
    const=10,
    default=10,
    help="Опция для создания лог файла не по умолчанию"
)
config_args = parser.parse_args()

# Настройка логирования
if __name__ == "__main__":  # Если программа запущена как скрипт, то
    logger = logging.getLogger("root")  # присваивем имя логеру root
else:
    logger = logging.getLogger(__name__)  # а если запущен как модуль, то даём название логеру, как у модуля
# Настройка логирования в файл
logging.basicConfig(
    filename=config_args.log_file,  # Название файла лога
    level=int(config_args.log_level),  # Уровень логирования
    format='%(levelname)s - %(name)s - %(asctime)s: %(message)s',  # Задаём формат записи
    datefmt="%Y-%m-%d %H:%M:%S"  # Задаём формат даты и времени
)
# Выводим критические ошибки на консоль
handler_stdout = logging.StreamHandler(sys.stdout)  # Выводим данные на консоль
handler_stdout.setLevel(logging.ERROR)  # Задаём уровень выводимых ошибок критический, закрывающие программу
logger.addHandler(handler_stdout)  # Добавляем объект handler к logger

# Парсим конфигурационный файл
try:  # Ловим ошибки
    with open(config_args.config_file) as inf:  # Открывем конфигурационный файл.
        config = json.load(inf)  # Считываем файл и преобразовываем из формата json в словари и списки формата python
except FileNotFoundError:  # Ловим ошибку отсутствия файла
    logger.exception("Конфигурационный файл не найден")  # Логируем ошибку как уровень ERROR
    logging.shutdown()  # Закрываем логирование
    sys.exit(1)  # Прерываем программу с ошибкой 1
except Exception as ex:  # Ловим и выводим другие ошибки
    logger.exception('Неизвестная ошибка при парсинге конфигурационного файла.')  # Логируем ошибку как уровень ERROR
    logging.shutdown()  # Закрываем логирование
    sys.exit(2)  # Прерываем программу с ошибкой 2
logger.debug("Конфигурационный файл получен: {}".format(config))  # Логируем данные из конфигурационного файла

# Техническая переменная для отслеживания количества перезаписей
k = 0
"""
if config["daemon"]:  # Если в конфигурационном файле в daemon дано значение true, то
    with daemon.DaemonContext():  # запускаем демона
        while True:  # Бесконечный цикл повторения
            k += 1
            main()  # Выполняем основную часть программы
            logging.debug("Сон {}".format(config["timer"]))  # Логируем на сколько производится прерывание
            time.sleep(config["timer"])  # Ждём указанное в конфигурационном файле время
else:"""  # Если false, то
main()  # Выполняем программу один раз
logging.shutdown()  # Закрываем логирование

# Альфа версия (Не смотреть)--------------------------------------------------------------------------------------------

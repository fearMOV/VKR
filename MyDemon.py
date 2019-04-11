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
import datetime  # Для того чтобы узнать текущее время
import jsonschema  # Для валидации файла json


# Классы ошибок---------------------------------------------------------------------------------------------------------
class ErrorPostgreSQL(psycopg2.Error):
    pass


class ErrorRedis(redis.exceptions):
    pass


class ErrorConsul(consul.ConsulException):
    pass


# Функции---------------------------------------------------------------------------------------------------------------
def get_from_postgresql():
    # Подключаемся к PostgreSQL и делаем запросы
    with psycopg2.connect(**config["postgresql"]) as conn:  # Подключаемся к PostgreSQL
        logger.info("Подключение к БД PostgreSQL успешно")  # Логируем успешное подключение к PostgreSQL
        conn.set_client_encoding('UTF8')  # Деодируем получаемые данные в UNICODE
        with conn.cursor() as cursor:  # Открываем курсор для обращений к БД
            # Расчёт уровня сервиса SL
            cursor.execute(
                """with timed_answering_calls as (
                    select project_id as id_project, count(session_id)::float as tac
                    from ns_inbound_call_data
                    WHERE is_processed = True and is_processed_after_threshold = false
                    group by id_project
                ),
                queued_calls_without_irrelevant_missed_calls as (
                    SELECT project_id, count(session_id)::float as qcwimc
                    FROM ns_inbound_call_data
                    WHERE is_unblocked = True and is_shortly_abandoned = false
                    group by project_id
                )
                select project_id, tac / qcwimc as sl
                from timed_answering_calls, queued_calls_without_irrelevant_missed_calls
                where project_id = id_project
                group by project_id, tac, qcwimc"""
            )
            sl_projects = {key: float(value) for key, value in cursor}
            logger.debug("Project_id: sl - {}".format(sl_projects))  # Логируем полученный словарь
            # Удаляем не нужные словари
            # del timed_answering_calls
            # del queued_calls_without_irrelevant_missed_calls
    return sl_projects


def get_from_redis():
    # Подключаемся к Redis и делаем запросы
    conn_redis = redis.StrictRedis(**config["redis"])  # Подключаемся к БД Redis
    logger.info("Подключение к БД Redis успешно")  # Логируем успешное подключение к БД Redis

    # Расчетное время ожидания (EWT) по каждой очереди
    project_ids = conn_redis.smembers("project_config:projects_set")  # Запрашиваем id проктов
    ewt = {}  # Создаём словарь для хранения информации полученной из Redis
    logger.debug("ID проектов из redis: {}".format(project_ids))  # Логируем полученыне id проектов
    for project_id in project_ids:  # Перебираем все полученные id проектов по одному
        project_id = project_id.decode()
        s = conn_redis.get("project_config:%s:mean_wait" % project_id)  # Делаем запрос к Redis, для получения ewt
        s = s.decode()  # Преобразуем полученные даныне в строку
        ewt[project_id] = s  # Записваем в словарь типа "project_id": "ewt"
    logger.debug("EWT: {}".format(ewt))  # Логируем полученынй словарь
    return ewt


def convert_to_json(sl_projects, ewt):
    # Переменная для формирования JSON файла
    data_json = dict()
    data_json["time"] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")  # Время записи данных
    # Заполняем переменную
    # Высчитываем SL инсталяции и записываем
    data_json["sl_instalation"] = sum(sl_projects.values()) / len(sl_projects.values())
    data_json["projects"] = []  # Будем создавать список проектов
    for key, value in sl_projects.items():  # Перебираем словарь с помощью items() получаем отдельно ключ и значение
        data_json["projects"].append({key: [{"sl": value, "ewt": ewt[key]}]})  # Создаём словарь
    logger.debug("Файл json: {}".format(data_json))  # Логируем созданынй словарь

    # Создаём json
    metrics_json = json.dumps(data_json, indent=4)  # Преобразовываем словарь в json
    return metrics_json


def send_report(metrics_json):
    # Закидывем json в consul
    c = consul.Consul(**config["consul"])  # Подключаемся к Consul
    key = "Balancer/" + platform.node()  # Создаём переменную ключа для передачи в Consul
    c.kv.put(key, metrics_json)  # Передаём сгенерированный json фалй по заданному ключу в Consul


def main():
    """
    Собираем json файл, беря данные из Redis и PostgreSQL, и выкладываем в Consul

    :return: Ничего не возвращает
    """
    try:
        sl_projects = get_from_postgresql()
        logger.info("Запросы к PostgreSQL выполнены")  # Логируем успешное выполнение запросов
        ewt = get_from_redis()
        metrics_json = convert_to_json(sl_projects, ewt)
        send_report(metrics_json)
    except ErrorPostgreSQL:
        logger.critical("Не получен доступ к PostgreSQL.")  # Выводим на экран ошибку
        logger.exception('')  # Логируем ошибку как уровень ERROR
        logging.shutdown()  # Закрываем логирование
        sys.exit(6)  # Прерываем программу с ошибкой 2
    except ErrorRedis:
        logger.critical("Не получен доступ к Redis.")  # Выводим на экран ошибку
        logger.exception('')  # Логируем ошибку как уровень ERROR
        logging.shutdown()  # Закрываем логирование
        sys.exit(7)  # Прерываем программу с ошибкой 2
    except ZeroDivisionError:
        logger.critical("При получение SL из PostgreSQL не были получнены данные")  # Выводим на экран ошибку
        logger.exception('')  # Логируем ошибку как уровень ERROR
        logging.shutdown()  # Закрываем логирование
        sys.exit(8)  # Прерываем программу с ошибкой 2
    except json.JSONDecodeError:
        logger.critical("Ошибка при преобразовании данных в json формат.")  # Выводим на экран ошибку
        logger.exception('')  # Логируем ошибку как уровень ERROR
        logging.shutdown()  # Закрываем логирование
        sys.exit(9)  # Прерываем программу с ошибкой 2
    except ErrorConsul:
        logger.critical("Ошибка при работе с Consul.")  # Выводим на экран ошибку
        logger.exception('')  # Логируем ошибку как уровень ERROR
        logging.shutdown()  # Закрываем логирование
        sys.exit(10)  # Прерываем программу с ошибкой 2
    except Exception:
        logger.critical("Неизвестная ошибка")  # Выводим на экран ошибку
        logger.exception('')  # Логируем ошибку как уровень ERROR
        logging.shutdown()  # Закрываем логирование
        sys.exit(11)  # Прерываем программу с ошибкой 2


# Тело программы--------------------------------------------------------------------------------------------------------
# Парсим аргументы из консоли, переданные при запуске скрипта
try:
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
    handler_stdout.setLevel(logging.CRITICAL)  # Задаём уровень выводимых ошибок критический, закрывающие программу
    logger.addHandler(handler_stdout)  # Добавляем объект handler к logger
except IOError:
    logger.critical("Ошибка при открытии/создании файла логов")  # Выводим на экран ошибку
    logger.exception("")  # Логируем ошибку как уровень ERROR
    logging.shutdown()  # Закрываем логирование
    sys.exit(1)  # Прерываем программу с ошибкой 1
except Exception:
    logger.critical("Ошибка при создании логера или парсинга аргументов из консоли.")  # Выводим на экран ошибку
    logger.exception('')  # Логируем ошибку как уровень ERROR
    logging.shutdown()  # Закрываем логирование
    sys.exit(2)  # Прерываем программу с ошибкой 2

# Схема файла json, которую мы ожидаем
schema_json = {
    "type": "object",
    "properties": {
        "demon": {"type": "boolean"},
        "timer": {"type": "integer"},
        "redis": {
            "type": "object",
            "properties": {
                "host": {"type": "string", "pattern": "^(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])){3}$|localhost$"},
                "port": {"type": "integer"},
                "db": {"type": "integer"}
            }
        },
        "postgresql": {
            "type": "object",
            "properties": {
                "dbname": {"type": "string"},
                "host": {"type": "string", "pattern": "^(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])){3}$|localhost$"},
                "port": {"type": "integer"},
                "user": {"type": "string"},
                "password": {"type": "string"}
            }
        },
        "consul": {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer"},
                "dc": {"type": "string"}
            }
        }
    }
}

# Парсим конфигурационный файл
try:  # Ловим ошибки
    with open(config_args.config_file) as inf:  # Открывем конфигурационный файл.
        config = json.load(inf)  # Считываем файл и преобразовываем из формата json в словари и списки формата python
        jsonschema.validate(config, schema_json)  # Производим валидацию полученного файла json
        logger.info("Конфигурационный файл получен")
except IOError:  # Ловим ошибки связаные с файлом
    logger.critical("Ошибка при открытии конфигурационного файла")
    logger.exception("")  # Логируем ошибку как уровень ERROR
    logging.shutdown()  # Закрываем логирование
    sys.exit(3)  # Прерываем программу с ошибкой 1
except jsonschema.exceptions.ValidationError:
    logger.critical("Файл конфигурации не прошёл валидацию")
    logger.exception("Ошибка валидации")
    logging.shutdown()
    sys.exit(4)
except Exception:  # Ловим и выводим другие ошибки
    logger.critical("Неизвестная ошибка при парсинге конфигурационного файла.")  # Выводим на экран ошибку
    logger.exception('')  # Логируем ошибку как уровень ERROR
    logging.shutdown()  # Закрываем логирование
    sys.exit(5)  # Прерываем программу с ошибкой 2
logger.debug("Конфигурационный файл получен: {}".format(config))  # Логируем данные из конфигурационного файла

# Техническая переменная для отслеживания количества перезаписей
"""
if config["daemon"]:  # Если в конфигурационном файле в daemon дано значение true, то
    with daemon.DaemonContext():  # запускаем демона
        while True:  # Бесконечный цикл повторения
            main(config)  # Выполняем основную часть программы
            logging.debug("Сон {}".format(config["timer"]))  # Логируем на сколько производится прерывание
            time.sleep(config["timer"])  # Ждём указанное в конфигурационном файле время
else:"""  # Если false, то
main()  # Выполняем программу один раз
logging.shutdown()  # Закрываем логирование
sys.exit(0)

# Альфа версия (Не смотреть)--------------------------------------------------------------------------------------------

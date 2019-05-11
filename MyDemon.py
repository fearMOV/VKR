"""
Программа собирает метрики из Redis и PostgreSQL, и выкладываем в Consul в формате json
"""

__author__ = "_FEAR_MOV_"
__version__ = 1.0

import psycopg2
from psycopg2.pool import SimpleConnectionPool
import redis
import sys
import json
import consul
import platform
import logging
import argparse
import datetime
import jsonschema
import time
import signal

start_time = time.time()


class Signals:
    """
    Определяем действия на системные сигналы.

    Программа реагирует на три сигнала:
    SIGINT - завершает программу по завершении цикла.
    SIGTERM - завершает программу по завершению цикла.
    SIGHUP - переопределяет конфигурационные данные и аргументы программы.
    """
    kill_now = False
    restart_now = False

    def __init__(self):
        """
        Инициализируем сигналы.
        """
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        # signal.signal(signal.SIGHUP, self.restart_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True

    # def restart_gracefully(self, signum, frame):
    #     self.restart_now = True


def arg_parse():
    """
    Парсим аргументы.

    Принимаемые аргументы:
    -h Информация об аргументах, которые принимает программа.

    -cf Указываем путь к конфигурационному файлу, по умолчанию ищет config.json в текущей папке программы.

    -lf Указываем путь к файлу логов, по умолчанию ищет sample.log в текущей папке программы.

    -ll Указываем уровень логирования, по умолчанию 10 - Debug. Есть ещё 20 - Info, 40 - Error, 50 - Critical.

    :return: возвращаем объект с тремя переменными config_file, log_file, log_level.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-cf",
        "--config_file",
        nargs="?",
        const="config.json",
        default="config.json",
        help="Опция для подключения конфигурационного файла, вне папки со скриптом"
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
    return parser.parse_args()


def create_logger():
    """
    Подключаем логирование.

    Всё выводится в файл и критические ошибки дублируются на консоль.

    :return: функция ничего не возващает.
    """
    # Выводим критические ошибки на консоль
    handler_stdout = logging.StreamHandler()
    handler_stdout.setLevel(logging.CRITICAL)
    handler_stdout.setFormatter(logging.Formatter('%(levelname)s - %(name)s - %(asctime)s: %(message)s'))
    logger.addHandler(handler_stdout)
    # Настройка логирования в файл
    logging.basicConfig(
        filename=config_args.log_file,
        level=int(config_args.log_level),
        format='%(levelname)s - %(name)s - %(asctime)s: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def config_validate():
    """
    Парсим конфигурационный файл и производим его валидацию.

    :return: возвращает переменную типа словарь с данными о конфигурации.
    """
    schema_json = {
        "type": "object",
        "properties": {
            "demon": {"type": "boolean"},
            "waiting-time": {"type": "integer"},
            "period": {"type": "integer"},
            "redis": {
                "type": "object",
                "properties": {
                    "host": {"type": "string",
                             "pattern": "^(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])){3}$|^localhost$"},
                    "port": {"type": "integer"},
                    "db": {"type": "integer"}
                }
            },
            "postgresql-naumen-db": {
                "type": "object",
                "properties": {
                    "dbname": {"type": "string"},
                    "host": {"type": "string",
                             "pattern": "^(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])){3}$|^localhost$"},
                    "port": {"type": "integer"},
                    "user": {"type": "string"},
                    "password": {"type": "string"}
                }
            },
            "postgresql-naumen-reports-db": {
                "type": "object",
                "properties": {
                    "dbname": {"type": "string"},
                    "host": {"type": "string",
                             "pattern": "^(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])){3}$|^localhost$"},
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
    with open(config_args.config_file) as inf:
        config_json = json.load(inf)
        jsonschema.validate(config_json, schema_json)
        logger.info("Configuration file received.")
    return config_json


def get_sl():
    """
    Подключаемся к PostgreSQL и делаем запрос для получения уровня сервиса (SL) для каждой очереди.

    :return: возвращает полученные из PostgreSQL данные в виде словаря, типа project_id: sl.
    """
    logger.info("Connection to PostgreSQL DB successfully.")
    conn_db.set_client_encoding('UTF8')
    with conn_db.cursor() as cursor:
        cursor.execute(
            """WITH timed_answering_calls AS (
                SELECT project_id AS id_project, count(session_id)::float AS tac
                FROM ns_inbound_call_data
                WHERE is_processed = True and is_processed_after_threshold = false
                GROUP BY id_project
            ),
            queued_calls_without_irrelevant_missed_calls AS (
                SELECT project_id, count(session_id)::float AS qcwimc
                FROM ns_inbound_call_data
                WHERE is_unblocked = True AND is_shortly_abandoned = false
                GROUP BY project_id
            )
            SELECT project_id, tac / qcwimc AS sl
            FROM timed_answering_calls, queued_calls_without_irrelevant_missed_calls
            WHERE project_id = id_project
            GROUP BY project_id, tac, qcwimc"""
        )
        sl_projects = {key: float(value) for key, value in cursor}
        logger.debug("Project_id: sl - {}".format(sl_projects))
    return sl_projects


def get_ewt(project_ids):
    """
    Подключаемся к Redis и делаем запрос для получения расчетного времени ожидания (EWT) по каждой очереди.

    :return: возвращает полученные из Redis данные в виде словаря, типа project_id: ewt.
    """
    print(project_ids)
    # Подключаемся к Redis и делаем запросы
    conn_redis = redis.StrictRedis(**config["redis"])
    logger.info("Connecting to the Redis database successfully.")

    # Расчетное время ожидания (EWT) по каждой очереди
    ewt = {}
    logger.debug("ID projects from redis: {}".format(project_ids))
    for project_id in project_ids:
        s = conn_redis.get("project_config:%s:mean_wait" % project_id)
        s = s.decode()
        ewt[project_id] = s
    logger.debug("EWT: {}".format(ewt))
    logger.info("Redis data retrieved")
    return ewt


def get_qualification(project_ids):
    print(project_ids)
    qualification = {}
    logins = set()
    conn_reports_db.set_client_encoding('UTF8')
    with conn_reports_db.cursor() as cursor:
        sql = """
            WITH qualifications AS (
                SELECT projectuuid, personuuid, qualification
                FROM mv_participant_history
                WHERE roletype='participaints' AND
                    (qualification NOTNULL AND '0' < qualification AND qualification <= '10')
                    AND (begindate < '{datetime}') AND ( (enddate ISNULL) OR (enddate >= '{datetime}') )
                GROUP BY projectuuid, qualification, personuuid
            )
            SELECT projectuuid, qualification, login 
            FROM mv_employee, qualifications
            WHERE removed=false AND personuuid=uuid
            GROUP BY projectuuid, qualification, login
        """.format(datetime=datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
        cursor.execute(sql)
        for row in cursor:
            print(row)
            if row[0] not in qualification:
                qualification[row[0]] = {
                    10: [], 9: [], 8: [], 7: [], 6: [], 5: [], 4: [], 3: [], 2: [], 1: []
                }
                qualification[row[0]][row[1]] += [row[2]]
            else:
                qualification[row[0]][row[1]] += [row[2]]
            logins.add(row[2])
    print(qualification)
    print(logins)
    occupancy = {}
    with conn_db.cursor() as cursor:
        for login in logins:
            print(login)
            sql = """
                WITH waiting_time_and_call_handling AS(
                    SELECT login AS login_wtach, sum(duration)::float AS wtach
                    FROM ns_agent_sub_status_duration
                    WHERE login='{login}' AND sub_status!='redirect' AND collected_ts > '{datetime}'
                    GROUP BY login_wtach
                ),
                call_handling AS (
                    SELECT login AS login_ch, sum(duration)::float AS ch
                    FROM ns_agent_sub_status_duration
                    WHERE login='{login}' AND
                        (sub_status='ringing' OR sub_status='speaking' OR sub_status='wrapup')
                        AND collected_ts > '{datetime}'
                    GROUP BY login_ch
                )
                SELECT login_wtach AS login, coalesce(ch, 0) / wtach AS occupancy
                FROM waiting_time_and_call_handling LEFT JOIN call_handling ON login_wtach = login_ch
                GROUP BY login, occupancy
            """.format(
                login=login,
                datetime=(datetime.datetime.today() - datetime.timedelta(seconds=int(config["period"]))).strftime("%Y-%m-%d %H:%M:%S")
            )
            cursor.execute(sql)
            occupancy = {key: value for key, value in cursor}
        print(occupancy)
    logger.info("PostgreSQL data retrieved.")
    return qualification


def convert_to_json(sl_projects, ewt):
    # Переменная для формирования JSON файла
    data_json = dict()
    data_json["time"] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # Заполняем переменную
    # Высчитываем SL инсталяции и записываем
    data_json["sl_instalation"] = sum(sl_projects.values()) / len(sl_projects.values())
    data_json["projects"] = []
    for key, value in sl_projects.items():
        data_json["projects"].append({key: [{"sl": value, "ewt": ewt[key]}]})
    logger.debug("File json: {}".format(data_json))

    # Создаём json
    metrics_json = json.dumps(data_json, indent=4)
    return metrics_json


def send_report(metrics_json):
    """
    Передаём сформированный json в Consul.

    :param metrics_json: обязательный параметр, сформированная json переменная.
    :return: функия ничего не возвращает.
    """
    c = consul.Consul(**config["consul"])
    key = "Balancer/" + platform.node()
    c.kv.put(key, metrics_json)


def main():
    """
    Собираем json файл, беря данные из Redis и PostgreSQL, и выкладываем в Consul

    :return: Ничего не возвращает
    """
    try:
        sl_projects = get_sl()
        ewt = get_ewt(sl_projects.keys())
        qualification = get_qualification(sl_projects.keys())
        metrics_json = convert_to_json(sl_projects, ewt)
        send_report(metrics_json)
    except psycopg2.Error:
        logger.critical("Not accessed PostgreSQL.")
        logger.exception('')
        logging.shutdown()
        postgresql_naumendb_pool.closeall()
        sys.exit(6)
    except redis.RedisError:
        logger.critical("Not accessed Redis.")
        logger.exception('')
        logging.shutdown()
        postgresql_naumendb_pool.closeall()
        sys.exit(7)
    except ZeroDivisionError:
        logger.critical("No data was received when retrieving SL from PostgreSQL.")
        logger.exception('')
        logging.shutdown()
        postgresql_naumendb_pool.closeall()
        sys.exit(8)
    except json.JSONDecodeError:
        logger.critical("Error when converting data to json format.")
        logger.exception('')
        logging.shutdown()
        postgresql_naumendb_pool.closeall()
        sys.exit(9)
    except consul.ConsulException:
        logger.critical("Error when working with Consul.")
        logger.exception('')
        logging.shutdown()
        postgresql_naumendb_pool.closeall()
        sys.exit(10)
    except Exception:
        logger.critical("Unknown error.")
        logger.exception('')
        logging.shutdown()
        postgresql_naumendb_pool.closeall()
        sys.exit(11)


if __name__ == "__main__":
    logger = logging.getLogger("root")
else:
    logger = logging.getLogger(__name__)

sig = Signals()

while True:
    try:
        config_args = arg_parse()
        create_logger()
        config = config_validate()
        postgresql_naumendb_pool = psycopg2.pool.SimpleConnectionPool(
            0, 1, **config["postgresql-naumen-db"]
        )
        conn_db = postgresql_naumendb_pool.getconn()
        postgresql_naumenreportsdb_pool = psycopg2.pool.SimpleConnectionPool(
            0, 1, **config["postgresql-naumen-reports-db"]
        )
        conn_reports_db = postgresql_naumenreportsdb_pool.getconn()
    except argparse.ArgumentError:
        sys.exit(110)
    except argparse.ArgumentTypeError:
        sys.exit(111)
    except IOError:
        logger.critical("Error opening/creating files.")
        logger.exception("")
        logging.shutdown()
        sys.exit(1)
    except jsonschema.exceptions.ValidationError:
        logger.critical("Configuration file failed validation.")
        logger.exception("")
        logging.shutdown()
        sys.exit(4)
    except psycopg2.pool.PoolError:
        logger.critical("DB connection error")
        logger.exception("")
        logging.shutdown()
        sys.exit(20)
    except Exception:
        logger.critical("Unknown error.")
        logger.exception('')
        logging.shutdown()
        sys.exit(5)
    logger.debug("Configuration file received: {}".format(config))

    while config["daemon"]:
        main()
        logging.debug("Waiting {} seconds".format(config["waiting-time"]))
        print("--- %s seconds ---" % (time.time() - start_time))
        time.sleep(config["waiting-time"])
        if sig.kill_now:
            logger.info("The program is closed by a signal SIGTERM")
            sys.exit(0)
        # elif sig.restart_now:
            # sig.restart_now = False
            # logger.info("Program restarted by SIGHUP signal.")
            # break
    else:
        main()
        break
logger.info("Program complete")

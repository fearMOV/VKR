"""
Программа собирает метрики из Redis и PostgreSQL, и выкладываем в Consul в формате json
"""
__author__ = "_FEAR_MOV_"
__version__ = 0.8

import psycopg2
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


class Signals:
    kill_now = False
    restart_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        #signal.signal(signal.SIGHUP, self.restart_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True

    #def restart_gracefully(self, signum, frame):
        #self.restart_now = True


def arg_parse():
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
    # Настройка логирования
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
    # Схема файла json, которую мы ожидаем
    schema_json = {
        "type": "object",
        "properties": {
            "demon": {"type": "boolean"},
            "timer": {"type": "integer"},
            "redis": {
                "type": "object",
                "properties": {
                    "host": {"type": "string",
                             "pattern": "^(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1][0-9]{2}|[0-9]{2}|[0-9])){3}$|^localhost$"},
                    "port": {"type": "integer"},
                    "db": {"type": "integer"}
                }
            },
            "postgresql": {
                "type": "array",
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
    with psycopg2.connect(**config["postgresql-naumendb"]) as conn:
        logger.info("Connection to PostgreSQL DB successfully.")
        conn.set_client_encoding('UTF8')
        with conn.cursor() as cursor:
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
            logger.debug("Project_id: sl - {}".format(sl_projects))
    return sl_projects


def get_qualification():
    qualification = {}
    logins = set()
    with psycopg2.connect(**config["postgresql-naumenreportsdb"]) as conn:
        conn.set_client_encoding('UTF8')
        with conn.cursor() as cursor:
            sql = """
                WITH qualification AS (
                    SELECT projectuuid, personuuid, qualification
                    FROM mv_participant_history
                    WHERE roletype='participaints' and qualification notnull and roletype='participaints'
                    GROUP BY projectuuid, qualification, personuuid
                )
                SELECT projectuuid, personuuid, login, qualification
                FROM mv_employee, qualification
                WHERE removed=false and personuuid=uuid
                GROUP BY projectuuid, qualification, personuuid, login
            """
            cursor.execute(sql)
            for row in cursor:
                print(row)
                qualification[row[0]] = [row[1], row[2], row[3]]
                logins.add(row[2])
    print(qualification)
    print(logins)
    with psycopg2.connect(**config["postgresql-naumendb"]) as conn:
        with conn.cursor() as cursor:
            for login in logins:
                print(login)
                sql = """
                    SELECT login, status, sub_status, sum(duration)
                    FROM ns_agent_sub_status_duration
                    WHERE login='{0}' and sub_status='normal'
                    GROUP BY login, status, sub_status
                """.format(login)
                cursor.execute(sql)
                for row in cursor.fetchall():
                    print(row)
    logger.info("PostgreSQL data retrieved.")


def get_from_redis():
    # Подключаемся к Redis и делаем запросы
    conn_redis = redis.StrictRedis(**config["redis"])
    logger.info("Connecting to the Redis database successfully.")

    # Расчетное время ожидания (EWT) по каждой очереди
    project_ids = conn_redis.smembers("project_config:projects_set")
    ewt = {}
    logger.debug("ID projects from redis: {}".format(project_ids))
    for project_id in project_ids:
        project_id = project_id.decode()
        s = conn_redis.get("project_config:%s:mean_wait" % project_id)
        s = s.decode()
        ewt[project_id] = s
    logger.debug("EWT: {}".format(ewt))
    logger.info("Redis data retrieved")
    return ewt


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
    # Закидывем json в consul
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
        get_qualification()
        ewt = get_from_redis()
        metrics_json = convert_to_json(sl_projects, ewt)
        send_report(metrics_json)
    except psycopg2.Error:
        logger.critical("Not accessed PostgreSQL.")
        logger.exception('')
        logging.shutdown()
        sys.exit(6)
    except redis.RedisError:
        logger.critical("Not accessed Redis.")
        logger.exception('')
        logging.shutdown()
        sys.exit(7)
    except ZeroDivisionError:
        logger.critical("No data was received when retrieving SL from PostgreSQL.")
        logger.exception('')
        logging.shutdown()
        sys.exit(8)
    except json.JSONDecodeError:
        logger.critical("Error when converting data to json format.")
        logger.exception('')
        logging.shutdown()
        sys.exit(9)
    except consul.ConsulException:
        logger.critical("Error when working with Consul.")
        logger.exception('')
        logging.shutdown()
        sys.exit(10)
    except Exception:
        logger.critical("Unknown error.")
        logger.exception('')
        logging.shutdown()
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
    except Exception:
        logger.critical("Unknown error.")
        logger.exception('')
        logging.shutdown()
        sys.exit(5)
    logger.debug("Configuration file received: {}".format(config))

    while config["daemon"]:
        main()
        logging.debug("Сон {}".format(config["timer"]))
        time.sleep(config["timer"])
        if sig.kill_now:
            logger.info("The program is closed by a signal SIGTERM")
            sys.exit(0)
        #elif sig.restart_now:
            #killer.restart_now = False
            #logger.info("Program restarted by SIGHUP signal.")
            #break
    else:
        main()
        break

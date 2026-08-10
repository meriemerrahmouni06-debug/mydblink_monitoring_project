import os
from dotenv import load_dotenv
load_dotenv()
import json
import psycopg2
from kafka import KafkaConsumer

PG_CONN = {
    "host": "localhost",
    #pour plus de securite :
    "port":os.getenv('POSTGRES_RAW_PORT'),
    "dbname":os.getenv('POSTGRES_RAW_DB'),
    "user":os.getenv('POSTGRES_RAW_USER'),
    "password":os.getenv('POSTGRES_RAW_PASSWORD')
}
TOPICS = [
    #1 er topic 
    {
        "topic": "monitoring.servers",
        "table": "raw_servers",
        "insert_sql": """
            INSERT INTO raw_data.raw_servers (metric, source_instance, server_id, server_name, date_collecte)
            VALUES (%(metric)s, %(source_instance)s, %(Id)s, %(ServerName)s, %(DateCollecte)s)
            ON CONFLICT (server_name)
            DO UPDATE SET
                metric = EXCLUDED.metric,
                source_instance = EXCLUDED.source_instance,
                server_id = EXCLUDED.server_id,
                date_collecte = EXCLUDED.date_collecte,
                inserted_at = NOW()
        """,
    },
    # 2 eme topic
    {
        "topic": "monitoring.instances",
        "table": "raw_instances",
        "insert_sql": """
            INSERT INTO raw_data.raw_instances (metric, source_instance, instance_id, server_name, instance_name, date_collecte)
            VALUES (%(metric)s, %(source_instance)s, %(Id)s, %(ServerName)s, %(InstanceName)s, %(DateCollecte)s)
            ON CONFLICT (instance_name)
            DO UPDATE SET
                metric = EXCLUDED.metric,
                source_instance = EXCLUDED.source_instance,
                instance_id = EXCLUDED.instance_id,
                server_name = EXCLUDED.server_name,
                date_collecte = EXCLUDED.date_collecte,
                inserted_at = NOW()
        """,
    },
    # 3 eme topic :monitoring.databases
    {
        "topic": "monitoring.databases",
        "table": "raw_databases",
        "insert_sql": """
            INSERT INTO raw_data.raw_databases (metric, source_instance, database_id, server_name, instance_name, database_name, date_collecte)
            VALUES (%(metric)s, %(source_instance)s, %(Id)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s, %(DateCollecte)s)
            ON CONFLICT (source_instance, database_name)
            DO UPDATE SET
                metric = EXCLUDED.metric,
                database_id = EXCLUDED.database_id,
                server_name = EXCLUDED.server_name,
                instance_name = EXCLUDED.instance_name,
                date_collecte = EXCLUDED.date_collecte,
                inserted_at = NOW()
    """,
    },
    # 4 eme topic 
    {
        
    "topic": "monitoring.cpu",
    "table": "raw_cpu",
    "insert_sql": """
        INSERT INTO raw_data.raw_cpu (metric, source_instance, cpu_id, server_name, instance_name, cpu_idle, cpu_sql, date_collecte, date_collecte2)
        VALUES (%(metric)s, %(source_instance)s, %(id)s, %(ServerName)s, %(InstanceName)s, %(cpu_idle)s, %(cpu_sql)s, %(DateCollecte)s, %(DateCollecte2)s)
        ON CONFLICT (source_instance, cpu_id)
        DO UPDATE SET
                cpu_idle = EXCLUDED.cpu_idle,
                cpu_sql = EXCLUDED.cpu_sql,
                date_collecte = EXCLUDED.date_collecte,
                date_collecte2 = EXCLUDED.date_collecte2,
                inserted_at = NOW()
    """,
    },

]

def get_pg_connection():
    return psycopg2.connect(**PG_CONN)


def build_consumer():
    """Un seul consumer qui ecoute tous les topics de TOPICS en meme temps."""
    topic_names = [t["topic"] for t in TOPICS]
    return KafkaConsumer(
        *topic_names,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",   # lit depuis le debut , si aucun offset connu
        enable_auto_commit=True,        # offset
        group_id="monitoring-consumer-group",#à partir d'où ? reprendre la lecture
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
()
def main():
    consumer = build_consumer()
    pg_conn = get_pg_connection()
    pg_conn.autocommit = True
    cursor = pg_conn.cursor()

    topic_to_config = {t["topic"]: t for t in TOPICS}

    print("Recuperation des messages disponibles...")

    # poll() lit ce qui est disponible maintenant, attend au maximum 10 secondes,
    # puis rend la main -- contrairement a la boucle infinie precedente
    messages_batch = consumer.poll(timeout_ms=10000)

    total_traites = 0

    for topic_partition, messages in messages_batch.items():
        for message in messages:
            event = message.value
            topic = message.topic
            config = topic_to_config[topic]
            try:
                cursor.execute(config["insert_sql"], event)
                print(f"Insere/mis a jour dans {config['table']} : {event}")
                total_traites += 1
            except Exception as e:
                print(f"Erreur d'insertion pour le topic {topic} : {e}")

    print(f"Termine. {total_traites} message(s) traite(s).")
    consumer.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
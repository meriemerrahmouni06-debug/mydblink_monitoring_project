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
    # 5 eme topic 
        {
            
        "topic": "monitoring.memory",
        "table": "raw_memory",
        "insert_sql": """
            INSERT INTO raw_data.raw_memory (metric, source_instance, memory_id, server_name, instance_name, Total_OSMemory, AvalaibleMemory, date_collecte, date_collecte2)
            VALUES (%(metric)s, %(source_instance)s, %(id)s, %(ServerName)s, %(InstanceName)s, %(Total_OSMemory)s, %(AvalaibleMemory)s, %(DateCollecte)s, %(DateCollecte2)s)
            ON CONFLICT (source_instance, memory_id)
            DO UPDATE SET
                    Total_OSMemory = EXCLUDED.Total_OSMemory,
                    AvalaibleMemory= EXCLUDED. AvalaibleMemory,
                    date_collecte = EXCLUDED.date_collecte,
                    date_collecte2 = EXCLUDED.date_collecte2,
                    inserted_at = NOW()
        """,
        },
            # 6 eme topic 
{
    "topic": "monitoring.disk",
    "table": "raw_diskspace",
    "insert_sql": """
        INSERT INTO raw_data.raw_diskspace (metric, source_instance, disk_id, drive_name, instance_name, physical_netbios_name, total_space_gb, free_space_gb, date_collecte, date_collecte2)
        VALUES (%(metric)s, %(source_instance)s, %(id)s, %(Drive_name)s, %(InstanceName)s, %(PhysicalNetbiosName)s, %(Total_space_GB)s, %(Free_Space_GB)s, %(DateCollecte)s, %(DateCollecte2)s)
        ON CONFLICT (source_instance, disk_id)
        DO UPDATE SET
            drive_name = EXCLUDED.drive_name,
            instance_name = EXCLUDED.instance_name,
            physical_netbios_name = EXCLUDED.physical_netbios_name,
            total_space_gb = EXCLUDED.total_space_gb,
            free_space_gb = EXCLUDED.free_space_gb,
            date_collecte = EXCLUDED.date_collecte,
            date_collecte2 = EXCLUDED.date_collecte2,
            inserted_at = NOW()
    """,
    },
    {
    "topic": "monitoring.backup",
    "table": "raw_backupstatus",
    "insert_sql": """
        INSERT INTO raw_data.raw_backupstatus (metric, source_instance, server_name, instance_name, database_name, database_type, last_backup_date, backup_status, date_collecte)
        VALUES (%(metric)s, %(source_instance)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s, %(DatabaseType)s, %(LastBackupDate)s, %(BackupStatus)s, %(DateCollecte)s)
        ON CONFLICT (source_instance, database_name, date_collecte)
        DO UPDATE SET
            server_name = EXCLUDED.server_name,
            instance_name = EXCLUDED.instance_name,
            database_type = EXCLUDED.database_type,
            last_backup_date = EXCLUDED.last_backup_date,
            backup_status = EXCLUDED.backup_status,
            inserted_at = NOW()
    """,
},
    {
    "topic": "monitoring.servicestatus",
    "table": "raw_servicestatus",
    "insert_sql": """
        INSERT INTO raw_data.raw_servicestatus (metric, source_instance, service_id, server_name, instance_name, service_name, status, date_collecte)
        VALUES (%(metric)s, %(source_instance)s, %(id)s, %(ServerName)s, %(InstanceName)s, %(ServiceName)s, %(Status)s, %(DateCollecte)s)
        ON CONFLICT (source_instance, service_name, date_collecte)
        DO UPDATE SET
            server_name = EXCLUDED.server_name,
            instance_name = EXCLUDED.instance_name,
            status = EXCLUDED.status,
            inserted_at = NOW()
    """,
},
    {
    "topic": "monitoring.backupsdetails",
    "table": "raw_backupsdetails",
    "insert_sql": """
        INSERT INTO raw_data.raw_backupsdetails (metric, source_instance, backup_id, server_name, instance_name, database_name, recovery_model, full_start_date, full_duration, full_size, differential_start_date, differential_duration, differential_size, log_start_date, log_duration, log_size, worst_rpo_last_30_days, date_collecte)
        VALUES (%(metric)s, %(source_instance)s, %(ID)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s, %(RecoveryModel)s, %(Full_Start_Date)s, %(Full_Duration)s, %(Full_Size)s, %(Differential_Start_Date)s, %(Differential_Duration)s, %(Differential_Size)s, %(Log_Start_Date)s, %(Log_Duration)s, %(Log_Size)s, %(Worst_RPO_Last_30_Days)s, %(DateCollecte)s)
        ON CONFLICT (source_instance, database_name, date_collecte)
        DO UPDATE SET
            recovery_model = EXCLUDED.recovery_model,
            full_start_date = EXCLUDED.full_start_date,
            full_duration = EXCLUDED.full_duration,
            full_size = EXCLUDED.full_size,
            differential_start_date = EXCLUDED.differential_start_date,
            differential_duration = EXCLUDED.differential_duration,
            differential_size = EXCLUDED.differential_size,
            log_start_date = EXCLUDED.log_start_date,
            log_duration = EXCLUDED.log_duration,
            log_size = EXCLUDED.log_size,
            worst_rpo_last_30_days = EXCLUDED.worst_rpo_last_30_days,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.backuphistory",
    "table": "raw_backuphistory",
    "insert_sql": """
        INSERT INTO raw_data.raw_backuphistory (metric, source_instance, backup_id, server_name, instance_name, database_name, backup_type, start_date, duration, size_mb, copy_only, compressed, encrypted, location, date_collecte)
        VALUES (%(metric)s, %(source_instance)s, %(ID)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s, %(Type)s, %(StartDate)s, %(Duration)s, %(Size)s, %(CopyOnly)s, %(Compressed)s, %(Encrypted)s, %(Location)s, %(DateCollecte)s)
        ON CONFLICT (source_instance, backup_id)
        DO UPDATE SET
            duration = EXCLUDED.duration,
            size_mb = EXCLUDED.size_mb,
            copy_only = EXCLUDED.copy_only,
            compressed = EXCLUDED.compressed,
            encrypted = EXCLUDED.encrypted,
            location = EXCLUDED.location,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.indexes",
    "table": "raw_indexes",
    "insert_sql": """
        INSERT INTO raw_data.raw_indexes (
            metric, source_instance, server_name, instance_name, database_name,
            owner_name, table_name, index_name, index_id, filegroup_name,
            index_type, no_of_keys, index_size, used_size, free_size,
            rows_count, row_mod_ctr, original_fill_factor, date_collecte
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s,
            %(OwnerName)s, %(TableName)s, %(IndexName)s, %(IndexId)s, %(FileGroupName)s,
            %(Type)s, %(No_OfKeys)s, %(IndexSize)s, %(UsedSize)s, %(FreeSize)s,
            %(Rows)s, %(RowModCtr)s, %(OriginalFillFactor)s, %(DateCollecte)s
        )
        ON CONFLICT (source_instance, database_name,owner_name, table_name, index_name,date_collecte)
        DO UPDATE SET
            index_size = EXCLUDED.index_size,
            used_size = EXCLUDED.used_size,
            free_size = EXCLUDED.free_size,
            rows_count = EXCLUDED.rows_count,
            row_mod_ctr = EXCLUDED.row_mod_ctr,
            original_fill_factor = EXCLUDED.original_fill_factor,
            date_collecte = EXCLUDED.date_collecte,
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
    
    total_traites = 0
    
    # poll() lit ce qui est disponible maintenant, attend au maximum 10 secondes,
    # puis rend la main -- contrairement a la boucle infinie precedente
    #pour repeter lappel / chaque appel 500 lignes max .
    while True:
        messages_batch = consumer.poll(timeout_ms=10000, max_records=500)
        
        if not messages_batch:
            break  # plus rien a lire, on sort de la boucle

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
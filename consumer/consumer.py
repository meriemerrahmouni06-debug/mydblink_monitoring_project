import os
from dotenv import load_dotenv
load_dotenv()
import json
import psycopg2
from kafka import KafkaConsumer

PG_CONN = {
    "host": os.getenv("POSTGRES_RAW_HOST", "localhost"),
    #pour plus de securite :
    "port": "5432" if os.getenv("POSTGRES_RAW_HOST") else os.getenv("POSTGRES_RAW_PORT", "5436"),
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
        ON CONFLICT (source_instance, cpu_id,date_collecte)
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
            ON CONFLICT (source_instance, memory_id,date_collecte)
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
        ON CONFLICT (source_instance, disk_id,date_collecte)
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
        ON CONFLICT (source_instance, backup_id,date_collecte)
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
    { 
        "topic": "monitoring.fragmentation",
        "table": "raw_fragmentation",
        "insert_sql": """
        INSERT INTO raw_data.raw_fragmentation (
            metric, source_instance, physical_netbios_name, instance_name,
            database_name, database_id, full_obj_name, index_id, index_name,
            index_type_desc, index_depth, index_level, avg_fragmentation,
            fragment_count, rank_value, date_collecte, date_collecte2
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(PhysicalNetbiosName)s, %(InstanceName)s,
            %(DatabaseName)s, %(DatabaseID)s, %(full_obj_name)s, %(index_id)s, %(name)s,
            %(index_type_desc)s, %(index_depth)s, %(index_level)s, %(avg_fragmentation)s,
            %(fragment_count)s, %(Rank)s, %(DateCollecte)s, %(DateCollecte2)s
        )
        ON CONFLICT (source_instance, database_name, full_obj_name, index_id, date_collecte)
        DO UPDATE SET
            avg_fragmentation = EXCLUDED.avg_fragmentation,
            fragment_count = EXCLUDED.fragment_count,
            rank_value = EXCLUDED.rank_value,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.activitymonitor",
    "table": "raw_activitymonitor",
    "insert_sql": """
        INSERT INTO raw_data.raw_activitymonitor (
            metric, source_instance, activity_id, date_collecte, server_name, instance_name,
            session_id, user_process, user_name, database_name, application_name, host_name,
            wait_type, statement_text
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(ID)s, %(DateCollecte)s, %(ServerName)s, %(InstanceName)s,
            %(SessionID)s, %(UserProcess)s, %(User)s, %(DatabaseName)s, %(Application)s, %(Host)s,
            %(WaitType)s, %(Statement)s
        )
        ON CONFLICT (source_instance, activity_id)
        DO UPDATE SET
            wait_type = EXCLUDED.wait_type,
            statement_text = EXCLUDED.statement_text,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.region",
    "table": "raw_region",
    "insert_sql": """
        INSERT INTO raw_data.raw_region (
            metric, source_instance, region_id, date_collecte, server_name,
            ip_address, region, country, city, latitude, longitude
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(Id)s, %(DateCollecte)s, %(ServerName)s,
            %(IPAddress)s, %(Region)s, %(Country)s, %(City)s, %(Latitude)s, %(Longitude)s
        )
        ON CONFLICT (server_name)
        DO UPDATE SET
            region_id = EXCLUDED.region_id,
            date_collecte = EXCLUDED.date_collecte,
            ip_address = EXCLUDED.ip_address,
            region = EXCLUDED.region,
            country = EXCLUDED.country,
            city = EXCLUDED.city,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.datafiles",
    "table": "raw_datafiles",
    "insert_sql": """
        INSERT INTO raw_data.raw_datafiles (
            metric, source_instance, datafile_id, date_collecte, date_collecte2, instance_name,
            physical_netbios_name, database_name, logical_data_name, file_location,
            total_filesize_mb, file_type, filegroup_name
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(Id)s, %(DateCollecte)s, %(DateCollecte2)s, %(InstanceName)s,
            %(PhysicalNetbiosName)s, %(DatabaseName)s, %(LogicalDataName)s, %(FileLocation)s,
            %(TotalFileSize_MB)s, %(FileType)s, %(FileGroupName)s
        )
        ON CONFLICT (source_instance, datafile_id)
        DO UPDATE SET
            date_collecte = EXCLUDED.date_collecte,
            date_collecte2 = EXCLUDED.date_collecte2,
            total_filesize_mb = EXCLUDED.total_filesize_mb,
            file_type = EXCLUDED.file_type,
            filegroup_name = EXCLUDED.filegroup_name,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.logfiles",
    "table": "raw_logfiles",
    "insert_sql": """
        INSERT INTO raw_data.raw_logfiles (
            metric, source_instance, date_collecte, server_name, instance_name, database_name,
            log_file_name, filegroup_name, file_type, total_size, used_size, free_size,
            used_pct, free_pct, autogrow, growths_remaining, max_size, growth_inc, can_grow, file_path
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(DateCollecte)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s,
            %(File)s, %(FileGroup)s, %(Type)s, %(TotalSize)s, %(UsedSize)s, %(FreeSize)s,
            %(UsedPct)s, %(FreePct)s, %(AutoGrow)s, %(GrowthsRemaining)s, %(MaxSize)s, %(GrowthInc)s, %(CanGrow)s, %(Path)s
        )
        ON CONFLICT (source_instance, database_name, log_file_name, date_collecte)
        DO UPDATE SET
            total_size = EXCLUDED.total_size,
            used_size = EXCLUDED.used_size,
            free_size = EXCLUDED.free_size,
            used_pct = EXCLUDED.used_pct,
            free_pct = EXCLUDED.free_pct,
            growths_remaining = EXCLUDED.growths_remaining,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.filegroups",
    "table": "raw_filegroups",
    "insert_sql": """
        INSERT INTO raw_data.raw_filegroups (
            metric, source_instance, date_collecte, server_name, instance_name, database_name,
            filegroup_name, file_count, file_type, allocated_size_used, total_size_used,
            total_size, used_size, free_size, can_grow
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(DateCollecte)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s,
            %(FileGroup)s, %(FileCount)s, %(Type)s, %(AllocatedSizeUsed)s, %(TotalSizeUsed)s,
            %(TotalSize)s, %(UsedSize)s, %(FreeSize)s, %(CanGrow)s
        )
        ON CONFLICT (source_instance, database_name, filegroup_name, date_collecte)
        DO UPDATE SET
            file_count = EXCLUDED.file_count,
            allocated_size_used = EXCLUDED.allocated_size_used,
            total_size_used = EXCLUDED.total_size_used,
            total_size = EXCLUDED.total_size,
            used_size = EXCLUDED.used_size,
            free_size = EXCLUDED.free_size,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.tablesinfo",
    "table": "raw_tablesinfo",
    "insert_sql": """
        INSERT INTO raw_data.raw_tablesinfo (
            metric, source_instance, date_collecte, server_name, instance_name, database_name,
            owner_name, table_name, filegroup_name, table_size, reserved_size, used_size, free_size,
            percent_of_db, rows_count, reserved_memory, used_memory, number_of_partitions,
            compression_type, table_type
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(DateCollecte)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s,
            %(OwnerName)s, %(TableName)s, %(FileGroupName)s, %(TableSize)s, %(ReservedSize)s, %(UsedSize)s, %(FreeSize)s,
            %(PercentOfDB)s, %(Rows)s, %(ReservedMemory)s, %(UsedMemory)s, %(NumberOfPartitions)s,
            %(CompressionType)s, %(TableType)s
        )
        
        
        ON CONFLICT (source_instance, database_name, owner_name, table_name)
        DO UPDATE SET
            date_collecte = EXCLUDED.date_collecte,
            server_name = EXCLUDED.server_name,
            instance_name = EXCLUDED.instance_name,
            table_size = EXCLUDED.table_size,
            reserved_size = EXCLUDED.reserved_size,
            used_size = EXCLUDED.used_size,
            free_size = EXCLUDED.free_size,
            percent_of_db = EXCLUDED.percent_of_db,
            rows_count = EXCLUDED.rows_count,
            reserved_memory = EXCLUDED.reserved_memory,
            used_memory = EXCLUDED.used_memory,
            number_of_partitions = EXCLUDED.number_of_partitions,
            compression_type = EXCLUDED.compression_type,
            table_type = EXCLUDED.table_type,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.activetransactions",
    "table": "raw_activetransactions",
    "insert_sql": """
        INSERT INTO raw_data.raw_activetransactions (
            metric, source_instance, date_collecte, server_name, instance_name, database_name,
            sql_text, execution_count, cpu_time, total_elapsed_time, creation_time
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(DateCollecte)s, %(ServerName)s, %(InstanceName)s, %(DatabaseName)s,
            %(SqlText)s, %(ExecutionCount)s, %(CpuTime)s, %(TotalElapsedTime)s, %(CreationTime)s
        )
        ON CONFLICT (source_instance, database_name, creation_time, date_collecte)
        DO UPDATE SET
            execution_count = EXCLUDED.execution_count,
            cpu_time = EXCLUDED.cpu_time,
            total_elapsed_time = EXCLUDED.total_elapsed_time,
            inserted_at = NOW()
    """,
},
{
    "topic": "monitoring.querybyduration",
    "table": "raw_querybyduration",
    "insert_sql": """
        INSERT INTO raw_data.raw_querybyduration (
            metric, source_instance, query_id, date_collecte, date_collecte2, server_name, instance_name,
            session_id, duration_ms, database_id, cpu_time_ms, wait_time, logical_reads, statement_text
        )
        VALUES (
            %(metric)s, %(source_instance)s, %(id)s, %(DateCollecte)s, %(DateCollecte2)s, %(ServerName)s, %(InstanceName)s,
            %(session_id)s, %(duration_ms)s, %(database_id)s, %(cpu_time_ms)s, %(wait_time)s, %(logical_reads)s, %(statement_text)s
        )
        ON CONFLICT (source_instance, query_id)
        DO UPDATE SET
            duration_ms = EXCLUDED.duration_ms,
            cpu_time_ms = EXCLUDED.cpu_time_ms,
            wait_time = EXCLUDED.wait_time,
            logical_reads = EXCLUDED.logical_reads,
            inserted_at = NOW()
    """,
},


]

def get_pg_connection():
    return psycopg2.connect(**PG_CONN)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
def build_consumer():
    """Un seul consumer qui ecoute tous les topics de TOPICS en meme temps."""
    topic_names = [t["topic"] for t in TOPICS]
    return KafkaConsumer(
        *topic_names,
        #bootstrap_servers="localhost:9092",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",   # lit depuis le debut , si aucun offset connu
        enable_auto_commit=True,        # offset
        group_id="monitoring-consumer-group",#à partir d'où ? reprendre la lecture
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

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
    total_traites = 0
    polls_vides = 0
    maximum_polls_vides = 3

    while polls_vides < maximum_polls_vides:
        messages_batch = consumer.poll(
            timeout_ms=10000,
            max_records=500
    )

        if not messages_batch:
            polls_vides += 1
            print(
                f"Aucun message reçu "
                f"({polls_vides}/{maximum_polls_vides})"
            )
            continue

        polls_vides = 0

        for topic_partition, messages in messages_batch.items():
            print(
                f"Lecture du topic {topic_partition.topic}, "
                f"partition {topic_partition.partition} : "
                f"{len(messages)} message(s)"
            )

            for message in messages:
                event = message.value
                topic = message.topic
                config = topic_to_config[topic]

                try:
                    cursor.execute(config["insert_sql"], event)

                    print(
                        f"Inséré dans {config['table']} : "
                        f"offset={message.offset}"
                    )

                    total_traites += 1

                except Exception as e:
                    print(
                        f"Erreur d'insertion pour le topic "
                        f"{topic} : {e}"
                    )
    print(f"Termine. {total_traites} message(s) traite(s).")
    consumer.close()
    pg_conn.close()

if __name__ == "__main__":
    main()
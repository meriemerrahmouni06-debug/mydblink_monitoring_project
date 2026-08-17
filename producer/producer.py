import pyodbc
import json
import os #importer depuis .env les mots de passe pour la connexion.
from dotenv import load_dotenv
load_dotenv()
#il faut pour le mode incremental , checkpoint..
import psycopg2
#pour maintenant envoyer a kafka :
from kafka import KafkaProducer
#pour gerer les donnees decimal :
from decimal import Decimal

#configuration de connexion avec PostgreSQL , pour mode incremental .]
#pour faire consultation ou je suis , derniere DateCollecte .
PG_CONN = {
    "host": "localhost",
    "port": os.getenv("POSTGRES_RAW_PORT"),
    "dbname": os.getenv("POSTGRES_RAW_DB"),
    "user": os.getenv("POSTGRES_RAW_USER"),
    "password": os.getenv("POSTGRES_RAW_PASSWORD"),
}
# ============================================================
# CONFIGURATION DES INSTANCES SQL SERVER
# ============================================================

INSTANCES = [
    {
        "name": "DESKTOP-TGCM8B5",
        #ici c'est deja il y a une relation de confiance windows ,
        #  le mode  "Trusted_Connection=yes;" suffit , sont dans meme domaine Windows .
        "conn_str": (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=192.168.1.27;"
            "DATABASE=msdb;"
            "Trusted_Connection=yes;"
        ),
    },
    {
        "name": "DESKTOP-F9GKO0O",
        #il n' y a pas ici : une relation de confiance windows pas comme la 1 ere instance 
        #"conn_str": (
            #"DRIVER={ODBC Driver 17 for SQL Server};"
            #"SERVER=DESKTOP-F9GKO0O;"
            #"DATABASE=msdb;"
            #"Trusted_Connection=yes;"
        # ),
        "conn_str": (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-F9GKO0O;"
    "DATABASE=msdb;"
    
    f"UID={os.getenv('SQL_UID')};"
    f"PWD={os.getenv('SQL_PWD')};"
)

    },
]

# ============================================================
# CONFIGURATION DES TABLES A TRAITER
# ============================================================
# Pour ajouter une nouvelle table plus tard (TBMonitorCPU, BackupHistory, etc.),
# il suffira d'ajouter un nouveau dictionnaire dans cette liste --
# pas besoin de reecrire le reste du script.

TABLES = [
    # 1 ere table servers.
    {
        "table_name": "[msdb].[DBMonitor].[servers]",
        "topic": "monitoring.servers",
        "metric": "servers",
        "select_columns": "Id, ServerName, DateCollecte",
        "mode": "full_snapshot",   # table de reference, peu de lignes -> on relit tout a chaque fois
        "frequency": "daily",
    },
    # voila , 2 eme table instances.
    {
        "table_name": "[msdb].[DBMonitor].[instances]",
        "topic": "monitoring.instances",
        "metric": "instances",
        "select_columns": "Id, ServerName, InstanceName, DateCollecte",
        "mode": "full_snapshot",
        "frequency": "daily",
    },
    #3 eme table databases.
    {
        "table_name": "[msdb].[DBMonitor].[databases]",
        "topic": "monitoring.databases",
        "metric": "databases",
        "select_columns": "Id, ServerName, InstanceName, DatabaseName, DateCollecte",
        "mode": "full_snapshot", #lit tout 
        "frequency": "daily",
    
    },
    #4 eme table TBMonitorCPU(mode incremental)
    {
    "table_name": "[msdb].[DBMonitor].[TBMonitorCPU]",
    "topic": "monitoring.cpu",
    "metric": "cpu",
    "select_columns": "id, DateCollecte, DateCollecte2, ServerName, InstanceName, cpu_idle, cpu_sql",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_cpu",
    "frequency": "30min",#pour separer (distinction).
    },
    # 5 eme table TBMonitorMemory
    {
    "table_name": "[msdb].[DBMonitor].[TBMonitorMemory]",
    "topic": "monitoring.memory",
    "metric": "memory",
    "select_columns": "id, DateCollecte, DateCollecte2, ServerName, InstanceName, Total_OSMemory, AvalaibleMemory",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_memory",
    "frequency": "30min",
    },
        # 6 eme table TBMonitorDisk
    {
    "table_name": "[msdb].[DBMonitor].[TBMonitorDiskSpace]",
    "topic": "monitoring.disk",
    "metric": "disk",
    "select_columns": "id, DateCollecte, DateCollecte2, Drive_name, InstanceName, PhysicalNetbiosName,Total_space_GB,Free_Space_GB",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_diskspace",
    "frequency": "30min",
    },

    {
    "table_name": "[msdb].[DBMonitor].[TBMonitorBackupStatus]",
    "topic": "monitoring.backup",
    "metric": "backup",
    "select_columns": "DateCollecte, ServerName, InstanceName, DatabaseName, DatabaseType, LastBackupDate, BackupStatus",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_backupstatus",
    "frequency": "30min",
    },
    {
    "table_name": "[msdb].[DBMonitor].[ServiceStatus]",
    "topic": "monitoring.servicestatus",
    "metric": "servicestatus",
    "select_columns": "id, DateCollecte, ServerName, InstanceName, ServiceName, Status",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_servicestatus",
    "frequency": "30min",
},
    {
    "table_name": "[msdb].[DBMonitor].[BackupsDetails]",
    "topic": "monitoring.backupsdetails",
    "metric": "backupsdetails",
    "select_columns": "ID, DateCollecte, ServerName, InstanceName, DatabaseName, RecoveryModel, Full_Start_Date, Full_Duration, Full_Size, Differential_Start_Date, Differential_Duration, Differential_Size, Log_Start_Date, Log_Duration, Log_Size, Worst_RPO_Last_30_Days",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_backupsdetails",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[BackupHistory]",
    "topic": "monitoring.backuphistory",
    "metric": "backuphistory",
    "select_columns": "ID, DateCollecte, ServerName, InstanceName, DatabaseName, Type, StartDate, Duration, Size, CopyOnly, Compressed, Encrypted, Location",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_backuphistory",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[IndexesInfo]",
    "topic": "monitoring.indexes",
    "metric": "indexes",
    "select_columns": "DateCollecte, ServerName, InstanceName, DatabaseName, OwnerName, TableName, IndexName, IndexId, FileGroupName, Type, No_OfKeys, IndexSize, UsedSize, FreeSize, Rows, RowModCtr, OriginalFillFactor",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_indexes",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[TBMonitorCollecteFragmentationTable]",
    "topic": "monitoring.fragmentation",
    "metric": "fragmentation",
    "select_columns": (
        "DateCollecte, DateCollecte2, PhysicalNetbiosName, InstanceName, "
        "DatabaseName, DatabaseID, full_obj_name, index_id, name, "
        "index_type_desc, index_depth, index_level, "
        "[AVG Fragmentation] AS avg_fragmentation, fragment_count, Rank"
    ),
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_fragmentation",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[ActivityMonitorProcesses]",
    "topic": "monitoring.activitymonitor",
    "metric": "activitymonitor",
    "select_columns": "ID, DateCollecte, ServerName, InstanceName, SessionID, UserProcess, [User], DatabaseName, Application, Host, WaitType, Statement",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_activitymonitor",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[Region]",
    "topic": "monitoring.region",
    "metric": "region",
    "select_columns": "Id, ServerName, DateCollecte, IPAddress, Region, Country, City, Latitude, Longitude",
    "mode": "full_snapshot",   # table de reference, tres peu de lignes -> relue entierement a chaque fois
    "frequency": "daily",
},
{
    "table_name": "[msdb].[DBMonitor].[Datafiles]",
    "topic": "monitoring.datafiles",
    "metric": "datafiles",
    "select_columns": "Id, DateCollecte, DateCollecte2, InstanceName, PhysicalNetbiosName, DatabaseName, LogicalDataName, FileLocation, TotalFileSize_MB, FileType, FileGroupName",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_datafiles",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[LogFilesTableInfo]",
    "topic": "monitoring.logfiles",
    "metric": "logfiles",
    "select_columns": "DateCollecte, ServerName, InstanceName, DatabaseName, [File], FileGroup, Type, TotalSize, UsedSize, FreeSize, UsedPct, FreePct, AutoGrow, GrowthsRemaining, MaxSize, GrowthInc, CanGrow, Path",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_logfiles",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[FileGroupsTableInfo]",
    "topic": "monitoring.filegroups",
    "metric": "filegroups",
    "select_columns": "DateCollecte, ServerName, InstanceName, DatabaseName, FileGroup, FileCount, Type, AllocatedSizeUsed, TotalSizeUsed, TotalSize, UsedSize, FreeSize, CanGrow",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_filegroups",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[TablesInfo]",
    "topic": "monitoring.tablesinfo",
    "metric": "tablesinfo",
    "select_columns": "DateCollecte, ServerName, InstanceName, DatabaseName, OwnerName, TableName, FileGroupName, TableSize, ReservedSize, UsedSize, FreeSize, PercentOfDB, Rows, ReservedMemory, UsedMemory, NumberOfPartitions, CompressionType, TableType",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_tablesinfo",
    "frequency": "30min",   # peut-etre "daily" ??
},
{
    "table_name": "[msdb].[DBMonitor].[ActiveTransactionsInfo]",
    "topic": "monitoring.activetransactions",
    "metric": "activetransactions",
    "select_columns": "DateCollecte, ServerName, InstanceName, DatabaseName, SqlText, ExecutionCount, CpuTime, TotalElapsedTime, CreationTime",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_activetransactions",
    "frequency": "30min",
},
{
    "table_name": "[msdb].[DBMonitor].[QueryByDurationTable]",
    "topic": "monitoring.querybyduration",
    "metric": "querybyduration",
    "select_columns": "id, DateCollecte, DateCollecte2, ServerName, InstanceName, session_id, duration_ms, database_id, cpu_time_ms, wait_time, logical_reads, statement_text",
    "mode": "incremental",
    "date_column": "DateCollecte",
    "postgres_table": "raw_querybyduration",
    "frequency": "30min",
},

]
#code en byte pour kafka comprend 
kafka_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)
# va etre appeler dans (extract_rows() ,si le mode est incremental , non sinon .
def get_last_collecte(table_name, instance_name):
    """Recupere la derniere date deja enregistree pour cette instance, dans Postgres."""
    conn = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT MAX(date_collecte) FROM raw_data.{table_name} WHERE source_instance = %s",
        (instance_name,)
    )
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result

#mise a jour de extract_rows pour qui lit si le mode incremental aussi .
def extract_rows(conn_str, table_config, instance_name):
    """Lit les lignes d'une table selon son mode (full_snapshot ou incremental)."""
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    if table_config["mode"] == "incremental":
        last_date = get_last_collecte(table_config["postgres_table"], instance_name)
        if last_date:
            query = f"""
                SELECT {table_config['select_columns']} 
                FROM {table_config['table_name']}
                WHERE {table_config['date_column']} > ?
            """
            cursor.execute(query, last_date)
        else:
            query = f"SELECT {table_config['select_columns']} FROM {table_config['table_name']}"
            cursor.execute(query)
    else:
        query = f"SELECT {table_config['select_columns']} FROM {table_config['table_name']}"
        cursor.execute(query)

    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    conn.close()
    return rows, columns

def row_to_event(row, columns, instance_name, metric_name):
    """Convertit une ligne SQL en dictionnaire pret pour JSON."""
    data = dict(zip(columns, row)) #associe chaque nom de colonne a sa valeur

    event = {"metric": metric_name, "source_instance": instance_name}

    for key, value in data.items():
        # datetime -> string ISO pour que ce soit serialisable en JSON
        if hasattr(value, "isoformat"):
            value = value.isoformat() #transform datetime en text
        elif isinstance(value, Decimal):
            value = float(value)#gerer les decimals , transforme decimal en text.
        event[key] = value
    return event


def process_table(table_config):
    print(f"\n=== Table : {table_config['table_name']} (topic {table_config['topic']}) ===")

    for instance in INSTANCES:
        name = instance["name"]
        print(f"\n--- {name} ---")


        try:
            rows, columns = extract_rows(instance["conn_str"], table_config,name)
        except Exception as e:
            print(f"Erreur de connexion sur {name} : {e}")
            continue

        if not rows:
            print("Aucune ligne trouvee.")
            continue

        for row in rows:
            event = row_to_event(row, columns, name, table_config["metric"])
            print(json.dumps(event, indent=2, ensure_ascii=False))
            #to send to kafka 
            kafka_producer.send(table_config["topic"], value=event)

    kafka_producer.flush()
#pour filtrer selon la frequence , dag 1 appelle par daily et lautre par 30min.    
def process_all_tables(frequency_filter):
    for table_config in TABLES:
        if table_config["frequency"] == frequency_filter:
            process_table(table_config)

def main():
    for table_config in TABLES:
        process_table(table_config)


if __name__ == "__main__":
    main()

# init.sql : 
CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE raw_data.raw_servers (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    server_id INT,
    server_name VARCHAR(255),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_servers UNIQUE (server_name)
);

CREATE TABLE raw_data.raw_instances (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    instance_id INT,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_instances UNIQUE (instance_name)
);

CREATE TABLE raw_data.raw_databases (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    database_id INT,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    database_name VARCHAR(255),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_databases UNIQUE (source_instance, database_name)
);

CREATE TABLE raw_data.raw_cpu (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    cpu_id INT,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    cpu_idle FLOAT,
    cpu_sql FLOAT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_cpu UNIQUE (source_instance, cpu_id)
);

CREATE TABLE raw_data.raw_memory (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    memory_id INT,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    total_osmemory FLOAT,
    avalaible_memory FLOAT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_memory UNIQUE (source_instance, memory_id)
);

CREATE TABLE raw_data.raw_diskspace (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    disk_id INT,
    drive_name VARCHAR(255),
    instance_name VARCHAR(255),
    physical_netbios_name VARCHAR(512),
    total_space_gb FLOAT,
    free_space_gb FLOAT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_diskspace UNIQUE (source_instance, disk_id)
);

CREATE TABLE raw_data.raw_backupstatus (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    database_name VARCHAR(255),
    database_type VARCHAR(100),
    last_backup_date TIMESTAMP,
    backup_status VARCHAR(100),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_backupstatus UNIQUE (source_instance, database_name, date_collecte)
);

CREATE TABLE raw_data.raw_servicestatus (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    service_id INT,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    service_name VARCHAR(255),
    status VARCHAR(100),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_servicestatus UNIQUE (source_instance, service_name, date_collecte)
);

CREATE TABLE raw_data.raw_backupsdetails (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    backup_id INT,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    database_name VARCHAR(255),
    recovery_model VARCHAR(100),
    full_start_date TIMESTAMP,
    full_duration FLOAT,
    full_size FLOAT,
    differential_start_date TIMESTAMP,
    differential_duration FLOAT,
    differential_size FLOAT,
    log_start_date TIMESTAMP,
    log_duration FLOAT,
    log_size FLOAT,
    worst_rpo_last_30_days FLOAT,
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_backupsdetails UNIQUE (source_instance, database_name, date_collecte)
);

CREATE TABLE raw_data.raw_backuphistory (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    backup_id INT,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    database_name VARCHAR(255),
    backup_type VARCHAR(50),
    start_date TIMESTAMP,
    duration FLOAT,
    size_mb FLOAT,
    copy_only BOOLEAN,
    compressed BOOLEAN,
    encrypted BOOLEAN,
    location VARCHAR(255),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_backuphistory UNIQUE (source_instance, backup_id)
);

CREATE TABLE raw_data.raw_indexes (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    database_name VARCHAR(255),
    owner_name VARCHAR(255),
    table_name VARCHAR(255),
    index_name VARCHAR(255),
    index_id INT,
    filegroup_name VARCHAR(255),
    index_type VARCHAR(100),
    no_of_keys INT,
    index_size FLOAT,
    used_size FLOAT,
    free_size FLOAT,
    rows_count BIGINT,
    row_mod_ctr BIGINT,
    original_fill_factor INT,
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_indexes UNIQUE (source_instance, database_name, table_name, index_name)
);

CREATE TABLE raw_data.raw_fragmentation (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    physical_netbios_name VARCHAR(512),
    instance_name VARCHAR(255),
    database_name VARCHAR(255),
    database_id INT,
    full_obj_name VARCHAR(510),
    index_id INT,
    index_name VARCHAR(255),
    index_type_desc VARCHAR(255),
    index_depth INT,
    index_level INT,
    avg_fragmentation FLOAT,
    fragment_count INT,
    rank INT,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fragmentation UNIQUE (source_instance, database_name, full_obj_name, index_id, date_collecte)
);
CREATE TABLE raw_data.raw_activitymonitor (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    activity_id INT,
    date_collecte TIMESTAMP,
    server_name VARCHAR(255),
    instance_name VARCHAR(255),
    session_id INT,
    user_process BOOLEAN,
    user_name VARCHAR(255),
    database_name VARCHAR(255),
    application_name VARCHAR(255),
    host_name VARCHAR(255),
    wait_type VARCHAR(255),
    statement_text TEXT,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_activitymonitor UNIQUE (source_instance, activity_id)
);

CREATE TABLE raw_data.raw_region (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    region_id INT,
    date_collecte TIMESTAMP,
    server_name VARCHAR(510),
    ip_address VARCHAR(100),
    region VARCHAR(510),
    country VARCHAR(510),
    city VARCHAR(510),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_region UNIQUE (server_name)
);

CREATE TABLE raw_data.raw_datafiles (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    datafile_id INT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    instance_name VARCHAR(256),
    physical_netbios_name VARCHAR(512),
    database_name VARCHAR(255),
    logical_data_name VARCHAR(255),
    file_location VARCHAR(255),
    total_filesize_mb NUMERIC(18,2),
    file_type VARCHAR(50),
    filegroup_name VARCHAR(255),
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_datafiles UNIQUE (source_instance, datafile_id)
);

CREATE TABLE raw_data.raw_logfiles (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    date_collecte TIMESTAMP,
    server_name VARCHAR(256),
    instance_name VARCHAR(512),
    database_name VARCHAR(256),
    log_file_name VARCHAR(256),
    filegroup_name VARCHAR(256),
    file_type VARCHAR(100),
    total_size BIGINT,
    used_size BIGINT,
    free_size BIGINT,
    used_pct NUMERIC(5,2),
    free_pct NUMERIC(5,2),
    autogrow VARCHAR(20),
    growths_remaining VARCHAR(100),
    max_size VARCHAR(100),
    growth_inc VARCHAR(100),
    can_grow VARCHAR(20),
    file_path VARCHAR(512),
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_logfiles UNIQUE (source_instance, database_name, log_file_name, date_collecte)
);

CREATE TABLE raw_data.raw_filegroups (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    date_collecte TIMESTAMP,
    server_name VARCHAR(256),
    instance_name VARCHAR(256),
    database_name VARCHAR(256),
    filegroup_name VARCHAR(256),
    file_count INT,
    file_type VARCHAR(256),
    allocated_size_used DOUBLE PRECISION,
    total_size_used DOUBLE PRECISION,
    total_size BIGINT,
    used_size BIGINT,
    free_size BIGINT,
    can_grow VARCHAR(20),
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_filegroups UNIQUE (source_instance, database_name, filegroup_name, date_collecte)
);

CREATE TABLE raw_data.raw_tablesinfo (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    date_collecte TIMESTAMP,
    server_name VARCHAR(256),
    instance_name VARCHAR(512),
    database_name VARCHAR(256),
    owner_name VARCHAR(256),
    table_name VARCHAR(256),
    filegroup_name VARCHAR(256),
    table_size NUMERIC(18,2),
    reserved_size NUMERIC(18,2),
    used_size NUMERIC(18,2),
    free_size NUMERIC(18,2),
    percent_of_db NUMERIC(5,2),
    rows_count BIGINT,
    reserved_memory NUMERIC(18,2),
    used_memory NUMERIC(18,2),
    number_of_partitions INT,
    compression_type VARCHAR(40),
    table_type VARCHAR(40),
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_tablesinfo UNIQUE (source_instance, database_name, owner_name, table_name, date_collecte)
);

CREATE INDEX idx_tablesinfo_instance_date ON raw_data.raw_tablesinfo (source_instance, date_collecte);

CREATE TABLE raw_data.raw_activetransactions (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    date_collecte TIMESTAMP,
    server_name VARCHAR(256),
    instance_name VARCHAR(256),
    database_name VARCHAR(256),
    sql_text TEXT,
    execution_count BIGINT,
    cpu_time BIGINT,
    total_elapsed_time BIGINT,
    creation_time TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_activetransactions UNIQUE (source_instance, database_name, creation_time, date_collecte)
);

CREATE INDEX idx_activetransactions_instance_date ON raw_data.raw_activetransactions (source_instance, date_collecte);

CREATE TABLE raw_data.raw_querybyduration (
    id SERIAL PRIMARY KEY,
    metric VARCHAR(50),
    source_instance VARCHAR(255),
    query_id INT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    server_name VARCHAR(256),
    instance_name VARCHAR(256),
    session_id INT,
    duration_ms INT,
    database_id INT,
    cpu_time_ms INT,
    wait_time INT,
    logical_reads INT,
    statement_text TEXT,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_querybyduration UNIQUE (source_instance, query_id)
);

SELECT * FROM raw_data.raw_querybyduration ;

SELECT COUNT(*),instance_name FROM  raw_data.raw_querybyduration
GROUP BY instance_name;

SELECT * FROM raw_data.raw_activetransactions ;
SELECT COUNT(*),instance_name FROM  raw_data.raw_activetransactions
GROUP BY instance_name;


CREATE INDEX idx_activetransactions_instance_date ON raw_data.raw_activetransactions (source_instance, date_collecte);

SELECT COUNT(*),instance_name FROM  raw_data.raw_tablesinfo 
GROUP BY instance_name;
CREATE INDEX idx_tablesinfo_instance_date ON raw_data.raw_tablesinfo (source_instance, date_collecte);

SELECT COUNT(*),instance_name FROM  raw_data.raw_filegroups
GROUP BY instance_name;

SELECT * FROM raw_data.raw_filegroups;

SELECT COUNT(*),instance_name FROM  raw_data.raw_datafiles
GROUP BY instance_name;


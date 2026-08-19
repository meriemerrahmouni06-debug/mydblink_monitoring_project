
-- ==========================================
-- ANALYTICS SCHEMA & TABLES
-- ==========================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. DIMENSION TABLES (Hierarchy)

CREATE TABLE IF NOT EXISTS analytics.dim_servers (
    server_id SERIAL PRIMARY KEY,
    server_name VARCHAR(255) NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_dim_servers UNIQUE (server_name)
);

CREATE TABLE IF NOT EXISTS analytics.dim_instances (
    instance_id SERIAL PRIMARY KEY,
    server_id INT NOT NULL REFERENCES analytics.dim_servers(server_id) ON DELETE CASCADE,
    instance_name VARCHAR(255) NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_dim_instances UNIQUE (server_id, instance_name)
);

CREATE TABLE IF NOT EXISTS analytics.dim_databases (
    database_id SERIAL PRIMARY KEY,
    instance_id INT NOT NULL REFERENCES analytics.dim_instances(instance_id) ON DELETE CASCADE,
    database_name VARCHAR(255) NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_dim_databases UNIQUE (instance_id, database_name)
);

-- 2. FACT TABLES (Metrics & Telemetry)

CREATE TABLE IF NOT EXISTS analytics.fact_cpu (
    id SERIAL PRIMARY KEY,
    instance_id INT NOT NULL REFERENCES analytics.dim_instances(instance_id) ON DELETE CASCADE,
    cpu_id INT,
    cpu_idle FLOAT,
    cpu_sql FLOAT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_cpu UNIQUE (instance_id, cpu_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_memory (
    id SERIAL PRIMARY KEY,
    instance_id INT NOT NULL REFERENCES analytics.dim_instances(instance_id) ON DELETE CASCADE,
    memory_id INT,
    total_osmemory FLOAT,
    avalaible_memory FLOAT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_memory UNIQUE (instance_id, memory_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_diskspace (
    id SERIAL PRIMARY KEY,
    instance_id INT NOT NULL REFERENCES analytics.dim_instances(instance_id) ON DELETE CASCADE,
    disk_id INT,
    drive_name VARCHAR(255),
    physical_netbios_name VARCHAR(512),
    total_space_gb FLOAT,
    free_space_gb FLOAT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_diskspace UNIQUE (instance_id, disk_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_backupstatus (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    database_type VARCHAR(100),
    last_backup_date TIMESTAMP,
    backup_status VARCHAR(100),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_backupstatus UNIQUE (database_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_servicestatus (
    id SERIAL PRIMARY KEY,
    instance_id INT NOT NULL REFERENCES analytics.dim_instances(instance_id) ON DELETE CASCADE,
    service_id INT,
    service_name VARCHAR(255),
    status VARCHAR(100),
    date_collecte TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_servicestatus UNIQUE (instance_id, service_name, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_backupsdetails (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    backup_id INT,
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
    CONSTRAINT uq_fact_backupsdetails UNIQUE (database_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_backuphistory (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    backup_id INT,
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
    CONSTRAINT uq_fact_backuphistory UNIQUE (database_id, backup_id)
);

CREATE TABLE IF NOT EXISTS analytics.fact_indexes (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
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
    CONSTRAINT uq_fact_indexes UNIQUE (database_id, table_name, index_name, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_fragmentation (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    physical_netbios_name VARCHAR(512),
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
    CONSTRAINT uq_fact_fragmentation UNIQUE (database_id, full_obj_name, index_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_activitymonitor (
    id SERIAL PRIMARY KEY,
    instance_id INT NOT NULL REFERENCES analytics.dim_instances(instance_id) ON DELETE CASCADE,
    database_id INT REFERENCES analytics.dim_databases(database_id) ON DELETE SET NULL,
    activity_id INT,
    date_collecte TIMESTAMP,
    session_id INT,
    user_process BOOLEAN,
    user_name VARCHAR(255),
    application_name VARCHAR(255),
    host_name VARCHAR(255),
    wait_type VARCHAR(255),
    statement_text TEXT,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_activitymonitor UNIQUE (instance_id, activity_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_region (
    id SERIAL PRIMARY KEY,
    server_id INT NOT NULL REFERENCES analytics.dim_servers(server_id) ON DELETE CASCADE,
    region_id INT,
    date_collecte TIMESTAMP,
    ip_address VARCHAR(100),
    region VARCHAR(510),
    country VARCHAR(510),
    city VARCHAR(510),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_region UNIQUE (server_id)
);

CREATE TABLE IF NOT EXISTS analytics.fact_datafiles (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    datafile_id INT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    physical_netbios_name VARCHAR(512),
    logical_data_name VARCHAR(255),
    file_location VARCHAR(255),
    total_filesize_mb NUMERIC(18,2),
    file_type VARCHAR(50),
    filegroup_name VARCHAR(255),
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_datafiles UNIQUE (database_id, datafile_id, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_logfiles (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    date_collecte TIMESTAMP,
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
    CONSTRAINT uq_fact_logfiles UNIQUE (database_id, log_file_name, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_filegroups (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    date_collecte TIMESTAMP,
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
    CONSTRAINT uq_fact_filegroups UNIQUE (database_id, filegroup_name, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_tablesinfo (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    date_collecte TIMESTAMP,
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
    CONSTRAINT uq_fact_tablesinfo UNIQUE (database_id, owner_name, table_name, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_activetransactions (
    id SERIAL PRIMARY KEY,
    database_id INT NOT NULL REFERENCES analytics.dim_databases(database_id) ON DELETE CASCADE,
    date_collecte TIMESTAMP,
    sql_text TEXT,
    execution_count BIGINT,
    cpu_time BIGINT,
    total_elapsed_time BIGINT,
    creation_time TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_activetransactions UNIQUE (database_id, creation_time, date_collecte)
);

CREATE TABLE IF NOT EXISTS analytics.fact_querybyduration (
    id SERIAL PRIMARY KEY,
    instance_id INT NOT NULL REFERENCES analytics.dim_instances(instance_id) ON DELETE CASCADE,
    database_id INT REFERENCES analytics.dim_databases(database_id) ON DELETE SET NULL,
    query_id INT,
    date_collecte TIMESTAMP,
    date_collecte2 TIMESTAMP,
    session_id INT,
    duration_ms INT,
    cpu_time_ms INT,
    wait_time INT,
    logical_reads INT,
    statement_text TEXT,
    inserted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_fact_querybyduration UNIQUE (instance_id, query_id, date_collecte)
);

-- 3. PERFORMANCE INDEXES 

CREATE INDEX IF NOT EXISTS idx_tablesinfo_db_date ON analytics.fact_tablesinfo (database_id, date_collecte);
CREATE INDEX IF NOT EXISTS idx_activetrans_db_date ON analytics.fact_activetransactions (database_id, date_collecte);
CREATE INDEX IF NOT EXISTS idx_querydur_inst_date ON analytics.fact_querybyduration (instance_id, date_collecte);

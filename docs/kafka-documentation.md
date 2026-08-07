
# Documentation — Pipeline Kafka du projet Monitoring

## 1. Objectif de ce document

Ce document explique le fonctionnement du pipeline **Producer → Kafka → Consumer → PostgreSQL_raw** mis en place pour collecter les métriques de monitoring SQL Server (tables de référence `servers`, `instances`, `databases`).

## 2. Vue d'ensemble de l'architecture

```
SQL Server Instance 1  ─┐
                         ├──► Producer Python ──► Kafka ──► Consumer Python ──► PostgreSQL (raw) 
SQL Server Instance 2  ─┘
```

- **80 procédures de monitoring** s'exécutent côté SQL Server toutes les 30 minutes et alimentent des tables de monitoring (CPU, disque, backup, blocages, waits, etc.).
- Le **Producer Python** lit ces tables sur chaque instance SQL Server, les transforme en JSON, et les publie sur Kafka.
- **Kafka** sert de bus d'événements découplé : un topic par métrique (`monitoring.servers`, `monitoring.instances`, `monitoring.databases`, etc.).
- Le **Consumer Python** écoute tous les topics et insère les données dans la couche brute (`raw_data`) de PostgreSQL.

## 3. Pourquoi Kafka ? Le principe de découplage

Le Producer et le Consumer ne communiquent **jamais directement** : ils passent uniquement par Kafka. Cela veut dire que :

- Le Producer peut tourner et publier des messages même si le Consumer est arrêté (les messages restent dans Kafka en attente).
- Le Consumer peut être redémarré sans perdre de données : Kafka retient sa position de lecture (l'*offset*) via le `group_id`.
- On peut ajouter d'autres consumers plus tard  sans toucher au Producer.

## 4. Le Producer (`producer.py`)

### 4.1 Configuration des instances SQL Server

```python
INSTANCES = [
    {
        "name": "DESKTOP-TGCM8B5",
        "conn_str": (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=192.168.1.27;"
            "DATABASE=msdb;"
            "Trusted_Connection=yes;"#meme domaine windows.
        ),
    },
    {
        "name": "DESKTOP-F9GKO0O",
        "conn_str": (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-F9GKO0O;"
            "DATABASE=msdb;"
            f"UID={os.getenv('SQL_UID')};"
            f"PWD={os.getenv('SQL_PWD')};"
        ),
    },
]
```

Chaque métrique est collectée sur **les deux instances**. La première instance utilise l'authentification Windows intégrée (`Trusted_Connection=yes`, même domaine Windows), la seconde instance utilise un compte SQL Server dédié (`UID`/`PWD`), chargé depuis `.env` via `python-dotenv` — aucun identifiant n'est en dur dans le code.

### 4.2 Configuration des tables (pilotage par configuration)

```python
TABLES = [
    {
        "table_name": "[msdb].[DBMonitor].[servers]",
        "topic": "monitoring.servers",
        "metric": "servers",
        "select_columns": "Id, ServerName, DateCollecte",
        "mode": "full_snapshot",
    },
    # instances, databases : même structure
]
```
**ajouter une nouvelle métrique ne nécessite pas de réécrire de logique**, juste d'ajouter une entrée dans cette liste. `mode: full_snapshot` signifie que la table entière est relue à chaque exécution (adapté aux tables de référence, peu volumineuses). Les futures métriques de type compteur (CPU, mémoire, ...) utiliseront un mode `incremental` basé sur la colonne `DateCollecte`, pour ne lire que les nouvelles lignes.

### 4.3 Connexion à Kafka et sérialisation

```python
kafka_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)
```

- `bootstrap_servers` : adresse du broker Kafka.
- `value_serializer` : Kafka ne transporte que des bytes. Cette fonction convertit automatiquement chaque dictionnaire Python en JSON puis en bytes avant l'envoi.

### 4.4 Extraction et transformation (`extract_rows`, `row_to_event`)

`extract_rows` ouvre une connexion pyodbc, exécute le `SELECT` défini pour la table, récupère les lignes et les noms de colonnes, puis ferme la connexion.

`row_to_event` transforme une ligne SQL en dictionnaire prêt pour le JSON : associe chaque colonne à sa valeur, convertit les `datetime` en chaîne ISO (`.isoformat()`), et ajoute deux champs de traçabilité : `metric` (nom de la métrique) et `source_instance` (nom de l'instance SQL Server d'origine).

### 4.5 Envoi vers Kafka (`process_table`)

```python
kafka_producer.send(table_config["topic"], value=event)
...
kafka_producer.flush()
```

- `send()` est **asynchrone** : il place le message dans une file d'envoi et rend la main immédiatement, sans attendre la confirmation de Kafka.
- `flush()`, appelé à la fin du traitement de chaque table, force l'envoi effectif de tous les messages en attente. Sans cet appel, le script pourrait se terminer avant que Kafka ait réellement reçu les messages, avec un risque de perte silencieuse.

## 5. Le Consumer (`consumer.py`)

### 5.1 Connexion PostgreSQL

```python
PG_CONN = {
    "host": "localhost",
     #pour plus de securite :
    "port": os.getenv('POSTGRES_RAW_PORT'),
    "dbname": os.getenv('POSTGRES_RAW_DB'),
    "user": os.getenv('POSTGRES_RAW_USER'),
    "password": os.getenv('POSTGRES_RAW_PASSWORD'),
}
```

Tous les identifiants sont lus depuis `.env` (aucun mot de passe en dur).

### 5.2 Configuration des topics (miroir de `TABLES` côté Producer)

```python
TOPICS = [
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
    # instances, databases : même principe, avec leur propre clé de conflit
]
```

Chaque entrée associe un topic Kafka à une table PostgreSQL et à sa requête d'insertion. Les placeholders nommés (`%(metric)s`, `%(Id)s`, ...) sont automatiquement remplis par psycopg2 à partir du dictionnaire JSON reçu de Kafka.

Point important : les requêtes utilisent `ON CONFLICT ... DO UPDATE` (upsert/merge). Comme le mode est `full_snapshot`, la table SQL Server entière est relue à chaque exécution du Producer — sans upsert(update+insert), on dupliquerait les lignes à chaque passage. La clé de conflit diffère selon la table (`server_name` pour table `servers`, `instance_name` pour table `instances` , ou le couple `source_instance` + `database_name` pour la table `databases`, car un même nom de base peut exister sur plusieurs instances).

### 5.3 Construction du Consumer

```python
def build_consumer():
    topic_names = [t["topic"] for t in TOPICS]
    return KafkaConsumer(
        *topic_names,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="monitoring-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
```

- `*topic_names` : un seul Consumer écoute **tous** les topics de la liste `TOPICS` simultanément.
- `auto_offset_reset="earliest"` : si ce `group_id` n'a jamais lu un topic, il repart du tout début plutôt que d'ignorer les messages déjà publiés.
- `enable_auto_commit=True` : Kafka retient automatiquement la position de lecture, pour ne jamais retraiter deux fois le même message après un redémarrage.
- `group_id` : identité du Consumer côté Kafka — c'est cette valeur qui détermine à partir d'où reprendre la lecture.(`offset`)

### 5.4 Boucle de consommation (`main`)

```python
for message in consumer:
    event = message.value
    topic = message.topic
    config = topic_to_config[topic]
    cursor.execute(config["insert_sql"], event)
```

Cette boucle est **infinie** : le Consumer reste en écoute active et traite chaque message dès son arrivée, quel que soit le topic. `pg_conn.autocommit = True` valide chaque insertion immédiatement, sans appel explicite à `.commit()`.

## 6. Résumé du flux

| Étape | Composant | Rôle |
|---|---|---|
| 1 | SQL Server (×2 instances) | Source des données de monitoring |
| 2 | Producer Python | Lecture, transformation en JSON, publication vers Kafka |
| 3 | Kafka | Bus d'événements découplé, un topic par métrique |
| 4 | Consumer Python | Écoute des topics, upsert dans PostgreSQL brut |
| 5 | PostgreSQL (schema : `raw_data`) | Couche brute, une table par métrique |


## 7. Ajouter une nouvelle métrique/table

Grâce au pilotage par configuration, ajouter une métrique (ex. `TBMonitorCPU`) ne nécessite pas de réécrire la logique du Producer ni du Consumer :

1. Ajouter une entrée dans `TABLES` (`producer.py`), avec `mode: incremental` pour les métriques volumineuses basées sur `DateCollecte`.
2. Créer la table correspondante dans PostgreSQL .
3. Ajouter une entrée dans `TOPICS` (`consumer.py`), avec la clé de conflit adaptée pour l'upsert.

## 8. Sécurité et bonnes pratiques appliquées

- Tous les identifiants (SQL Server, PostgreSQL) sont externalisés dans un fichier `.env`, chargé via `python-dotenv` — aucun mot de passe en dur dans le code.
- Le fichier `.env` est exclu du dépôt Git via `.gitignore`.

## 9. État d'avancement

Le pipeline complet (SQL Server → Producer → Kafka → Consumer → PostgreSQL brut) est validé de bout en bout pour les 3 tables de référence : `servers`, `instances`, `databases`, chacune collectée sur 2 instances SQL Server différentes. 
Pour ma prochaine étape : ajout des métriques en mode `incremental` (CPU, mémoire, backups ...).

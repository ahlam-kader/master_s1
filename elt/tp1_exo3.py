import psycopg2

REMOTE_DB = {
    "database": "etldb",
    "user": "deml",
    "password": "deml2025!",
    "host": "10.136.127.1",
    "port": "5432"
}

LOCAL_DB = {
    "database": "etldb",
    "user": "admin",
    "password": "adminx01",
    "host": "pgdb",
    "port": "5432"
}

def get_remote_data():
    try:
        conn = psycopg2.connect(**REMOTE_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT year, election, wilaya, moughataa, commune, candidate, nb_votes FROM rim_elections;")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        print(f"erreur de connexion a la base distante : {e}")
        return None

def create_local_schema():
    try:
        conn = psycopg2.connect(**LOCAL_DB)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS election (
            id SERIAL PRIMARY KEY,
            code VARCHAR(50) UNIQUE NOT NULL,
            year INT NOT NULL,
            label TEXT NOT NULL
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS voting_station (
            id SERIAL PRIMARY KEY,
            code VARCHAR(100) UNIQUE NOT NULL,
            label TEXT NOT NULL,
            wilaya VARCHAR(100),
            moughataa VARCHAR(100),
            commune VARCHAR(100)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            id SERIAL PRIMARY KEY,
            candidate VARCHAR(100) NOT NULL,
            nb_votes INT NOT NULL,
            election_id VARCHAR(50) REFERENCES election(code),
            voting_station_id VARCHAR(100) REFERENCES voting_station(code)
        );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("schema local cree avec succes !")
    except Exception as e:
        print(f"erreur lors de la creation du schema : {e}")

def migrate_data(data):
    if not data:
        print("aucune donnee a migrer.")
        return

    try:
        conn = psycopg2.connect(**LOCAL_DB)
        cursor = conn.cursor()

        for row in data:
            year, election, wilaya, moughataa, commune, candidate, nb_votes = row

            candidate = candidate[:100] if candidate else ""
            wilaya = wilaya[:100] if wilaya else ""
            moughataa = moughataa[:100] if moughataa else ""
            commune = commune[:100] if commune else ""

            election_code = f"{election}_{year}".replace(" ", "_")
            voting_station_code = f"{wilaya}_{moughataa}_{commune}".replace(" ", "_")

            cursor.execute("SELECT id FROM election WHERE code = %s;", (election_code,))
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO election (code, year, label) VALUES (%s, %s, %s);",
                    (election_code, year, election)
                )

            cursor.execute("SELECT id FROM voting_station WHERE code = %s;", (voting_station_code,))
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO voting_station (code, label, wilaya, moughataa, commune) VALUES (%s, %s, %s, %s, %s);",
                    (voting_station_code, commune, wilaya, moughataa, commune)
                )

            cursor.execute(
                "INSERT INTO votes (candidate, nb_votes, election_id, voting_station_id) VALUES (%s, %s, %s, %s);",
                (candidate, nb_votes, election_code, voting_station_code)
            )

        conn.commit()
        cursor.close()
        conn.close()
        print("migration des donnees terminee avec succes !")
    except Exception as e:
        print(f"erreur lors de la migration : {e}")

create_local_schema()
data = get_remote_data()
if data:
    migrate_data(data)

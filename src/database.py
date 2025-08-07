import sqlite3
from pathlib import Path
import json

class Database:
    def __init__(self, db_name=':memory:'):
        self.data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir.mkdir(parents=True, exist_ok=True)

        if db_name == ':memory:':
            print("Using in-memory database.")
            self.connection = sqlite3.connect(db_name)
        else:
            if not db_name.endswith('.db'):
                db_name += '.db'
            print(f"Connecting to database: {db_name}")
            db_path = self.data_dir / db_name
            self.connection = sqlite3.connect(str(db_path))

        self.cursor = self.connection.cursor()
        self.create_tables()

    def create_tables(self):
        with open(self.data_dir / 'schema.sql', 'r') as f:
            schema = f.read()
        self.cursor.executescript(schema)
        self.connection.commit()

    def close(self):
        self.connection.close()

    def insertEvent(self, event: dict):
        name = event.get('name')
        if not name:
            raise ValueError("Event must have a 'name' field.")

        event_data = {k: v for k, v in event.items() if k != 'name'}
        columns = ', '.join(event_data.keys())
        placeholders = ', '.join(['?'] * len(event_data))
        sql = f"INSERT INTO {name} ({columns}) VALUES ({placeholders})"
        self.cursor.execute(sql, tuple(event_data.values()))
        self.connection.commit()
        return self.cursor.lastrowid

    def fetch_event(self, event_id, event_name):
        sql = f"SELECT * FROM {event_name} WHERE _id = ?"
        self.cursor.execute(sql, (event_id,))
        row = self.cursor.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, row))

    def fetch_events(self, event_name):
        sql = f"SELECT * FROM {event_name}"
        self.cursor.execute(sql)
        return self.cursor.fetchall()
    
    def run_query(self, query: str):
        if not query.strip().lower().startswith("select"):
            raise ValueError("Only SELECT queries are allowed.")
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return json.dumps(result)
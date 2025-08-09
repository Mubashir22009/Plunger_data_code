from src.data import DataLoader
from src.events_generator import EventsGenerator
from src.database import Database
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import sqlite3
import time
import json
from src.metadata import cdt
from datetime import datetime
from src.fetcher.fetcher import fetcher_Main

def main():
    fetcher_Main()
    db_name = "events.db"
    db = Database(db_name)
    data_loader = DataLoader(Path.cwd() / "data")
    events_generator = EventsGenerator(data_loader, db)
    events_generator.generate_events()
    db.close()

    class SimpleSQLHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            query = post_data.decode()
            if not query:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'Missing query in request body')
                return
            try:
                # Create a new Database instance for this thread/handler
                thread_db = Database(db_name)
                result = thread_db.run_query(query)
                thread_db.close()
            except ValueError as ve:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(str(ve).encode())
                return
            except Exception as e:
                # Check for SQL error (sqlite3.DatabaseError or similar)
                if isinstance(e, sqlite3.DatabaseError):
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(str(e).encode())
                else:
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write(b'Database query failed or returned None')
                return
            if result is None:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b'Database query failed or returned None')
                return
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

    server = HTTPServer(('0.0.0.0', 8765), SimpleSQLHandler)
    print("Listening on port 8765...")
    server.timeout = 0.1
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
            pass
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.shutdown()
        server.server_close()
        # clean up db temporarily, just for testing
        # (Path.cwd() / 'data' / 'events.db').unlink(missing_ok=True)

if __name__ == "__main__":
    main()

from src.data import DataLoader
from src.events_generator import EventsGenerator
from src.database import Database
from pathlib import Path
from src.metadata import cdt
from datetime import datetime
from src.fetcher.fetcher import fetcher_Main

def main():
    fetcher_Main()
    db = Database('events.db')
    data_loader = DataLoader(Path.cwd() / "data")
    events_generator = EventsGenerator(data_loader)
    events = events_generator.generate_events()
    db.close()

if __name__ == "__main__":
    main()

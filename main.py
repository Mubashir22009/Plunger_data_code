from src.data import DataLoader
from src.events_generator import EventsGenerator
from src.database import Database
from pathlib import Path

def main():
    db = Database('events.db')
    data_loader = DataLoader(Path.cwd() / "data")
    events_generator = EventsGenerator(data_loader, db)
    events = events_generator.generate_events()
    #print(db.fetch_events('BASIC_PRESSURE_EVENTS'))
    #print(events)
    db.close()

if __name__ == "__main__":
    main()

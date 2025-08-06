from src.data import DataLoader
from src.events_generator import EventsGenerator
from pathlib import Path

def main():
    data_loader = DataLoader(Path.cwd() / "data")
    events_generator = EventsGenerator(data_loader)
    events = events_generator.generate_events()
    print(events)

if __name__ == "__main__":
    main()

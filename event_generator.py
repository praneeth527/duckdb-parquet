from datetime import datetime

from faker import Faker


class EventGenerator:
    def __init__(self):
        self.fake = Faker()

    def generate_event(self):
        return {
            'event_name': self.fake.bs(),
            'event_time': self.fake.date_time_between(start_date=datetime(2024, 8, 1),
                                                      end_date=datetime(2024, 8, 3)).isoformat(),
            'name': self.fake.name(),
            'city': self.fake.city(),
            'amount': self.fake.random_int(min=0, max=9999)
        }

    def generate_events(self, count):
        events = []
        for _ in range(count):
            events.append(self.generate_event())
        return events

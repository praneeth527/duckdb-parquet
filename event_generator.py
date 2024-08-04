from faker import Faker


class EventGenerator:
    def __init__(self):
        self.fake = Faker()

    def generate_event(self):
        return {
            'event_name': self.fake.bs(),
            'event_time': self.fake.date_time_this_year().isoformat(),
            'name': self.fake.name(),
            'city': self.fake.city(),
            'amount': self.fake.random_int(min=0, max=9999)
        }

    def generate_events(self, count):
        events = []
        for _ in range(count):
            events.append(self.generate_event())
        return events

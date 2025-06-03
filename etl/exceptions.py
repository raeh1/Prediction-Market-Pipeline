class Fetch_event_exception(Exception):
    def __init__(self, message):
        self.message = message

class Database_connection_exception(Exception):
    def __init__(self, message):
        self.message = message
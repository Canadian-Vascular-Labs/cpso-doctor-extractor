# state.py
class ScraperState:
    def __init__(self):
        self.seen_tasks = set()
        self.hard_stopped = set()

    def task_key(self, postal, params):
        return (postal, frozenset(params.items()))

    def seen(self, postal, params):
        key = self.task_key(postal, params)
        if key in self.seen_tasks:
            return True
        self.seen_tasks.add(key)
        return False

    def hard_stop(self, postal, params):
        key = self.task_key(postal, params)
        if key in self.hard_stopped:
            return True
        self.hard_stopped.add(key)
        return False

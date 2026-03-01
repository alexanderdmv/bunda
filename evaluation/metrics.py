from collections import Counter

class DecisionStats:
    def __init__(self):
        self.counter = Counter()

    def update(self, decision):
        self.counter[decision] += 1

    def report(self):
        total = sum(self.counter.values())
        print("---- STATS ----")
        for k, v in self.counter.items():
            print(f"{k}: {v} ({v/total:.2%})")

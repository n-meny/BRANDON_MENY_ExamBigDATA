from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from io import StringIO

class UserTagCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_user_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_user_tags(self, _, line):
        if line.startswith("userId"):
            return
        try:
            # Utilise csv.reader pour gérer les champs avec virgules ou guillemets
            reader = csv.reader(StringIO(line))
            fields = next(reader)
            if len(fields) >= 1:
                userID = fields[0]
                yield userID, 1
        except Exception:
            pass  # Optionnel : log des erreurs

    def reducer_count_tags(self, userID, counts):
        yield userID, sum(counts)

if __name__ == '__main__':
    UserTagCounter.run()

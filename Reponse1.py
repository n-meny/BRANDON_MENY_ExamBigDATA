# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from io import StringIO

class MovieTagCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_movie_tags(self, _, line):
        if line.startswith("userId"):
            return
        # Utilisation de csv.reader pour gérer les virgules dans les champs
        try:
            reader = csv.reader(StringIO(line))
            fields = next(reader)
            if len(fields) >= 2:
                movieID = fields[1]
                yield movieID, 1
        except Exception as e:
            # Optionnel : log si nécessaire
            pass

    def reducer_count_tags(self, movieID, counts):
        yield movieID, sum(counts)

if __name__ == '__main__':
    MovieTagCounter.run()

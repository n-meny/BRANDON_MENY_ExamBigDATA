# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieTagCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_movie_tags(self, _, line):
        if line.startswith("userId"):
            return
        try:
            fields = line.strip().split(',')
            if len(fields) >= 3:
                movieID = fields[1]
                yield movieID, 1
        except Exception as e:
            # Ligne ignorée si erreur
            pass

    def reducer_count_tags(self, movieID, counts):
        yield movieID, sum(counts)

if __name__ == '__main__':
    MovieTagCounter.run()

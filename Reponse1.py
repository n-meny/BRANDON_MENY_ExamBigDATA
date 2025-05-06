from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieTagCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_movie_tags(self, _, line):
        try:
            if line.startswith("userId"):
                return
            fields = line.strip().split(',')
            if len(fields) >= 3:
                movieId = fields[1]
                yield movieId, 1
        except Exception as e:
            # Affiche l'erreur dans les logs de sortie standard
            self.stderr.write(f"Erreur dans le mapper: {e}\n")

    def reducer_count_tags(self, movieId, counts):
        yield movieId, sum(counts)

if __name__ == '__main__':
    MovieTagCount.run()

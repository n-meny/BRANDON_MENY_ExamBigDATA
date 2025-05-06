
from mrjob.job import MRJob
from mrjob.step import MRStep

class UserTagCount(MRJob):

         def steps(self):
             return [
                MRStep(mapper=self.mapper_get_user_tags,
                    reducer=self.reducer_count_tags)
                ]

         def mapper_get_user_tags(self, _, line):
            if line.startswith("userId"):
                return
            fields = line.strip().split(',')
            if len(fields) >= 3:
                userId = fields[0]
                yield userId, 1

         def reducer_count_tags(self, userId, counts):
            yield userId, sum(counts)
if __name__ == '__main__':
        UserTagCount.run()   
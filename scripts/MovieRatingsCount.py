from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatingsCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_sort_ratings),
            MRStep(reducer=self.reducer_aggregate_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_sort_ratings(self, key, values):
        yield f'{sum(values):0>4d}', key

    def reducer_aggregate_ratings(self, key, values):
        for movie_id in values:
            yield movie_id, key

if __name__ == '__main__':
    MovieRatingsCount.run()


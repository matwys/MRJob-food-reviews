from mrjob.job import MRJob
from mrjob.step import MRStep


class MRReview(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]

    def mapper(self, _, line):
        (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator,
          HelpfulnessDenominator, Score, Time, Summary, Text) = line.split('\t')
        Score = int(Score)
        yield f'{Score:02d}', 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__=='__main__':
    MRReview.run()
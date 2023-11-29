from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w]+")


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
        yield None, len(WORD_RE.findall(Text))

    def combiner(self, key, values):
        yield key, max(values)

    def reducer(self, key, values):
        yield key, max(values)


if __name__=='__main__':
    MRReview.run()
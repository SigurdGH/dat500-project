from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRPrepro(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_init=self.first_mapper_init,
                mapper=self.first_mapper,
                mapper_final=self.first_mapper_final,
                reducer=self.first_reducer
            ),
            MRStep(
                mapper=self.second_mapper
            ),
            MRStep(
                mapper=self.third_mapper,
                reducer=self.third_reducer              
            ),
            MRStep(
                reducer=self.sort_reducer                
            )
        ]

    ### The first mappers take care of reducing the rows, but leaves some articles without and identifiable id ###
    def first_mapper_init(self):
        self.article = [] # Holds the CONTENT (value) of the Article
        self.article_id = -1 # Holds the ID (key) of the Article

        self.newArticleContent = "" # Needed since a new row is both our start and finish

    def first_mapper(self, _, line):
        line = line.strip()
        line_split = line.split("|")

        if len(line_split) > 1:
            self.article_id = int(line_split[0]) # Store new row information
            content = line_split[1]
            content = re.sub(r"[^a-zA-Z0-9 ]+", '', content)
            self.newArticleContent = content # Store new row information

            if len(self.article) > 0:
                yield self.article_id-1, ' '.join(self.article) # Yield old information
                self.article = [] # Check if necessary

            yield self.article_id, self.newArticleContent # Yield new information
        else:
            content = line
            content = re.sub(r"[^a-zA-Z0-9 ]+", '', content)
            self.article.append(content) # Read everything between rows

    def first_mapper_final(self): # If we never found a new row, we never yielded. Then just yield the body
        yield self.article_id, ' '.join(self.article)

    def first_reducer(self, id, content):
        if id != -1:
            yield id, ' '.join(content)


    
    # Makes shingles for each article
    def second_mapper(self, id, content):
        word_list = content.split(" ")
        word_list = [word for word in word_list if word]
        n = 2 # Amount of words in each shingle
        shingles = [word_list[i:i+n] for i in range(len(word_list)-(n-1))]
        yield id, shingles


    # Yields 1 for each time a shingle appears
    def third_mapper(self, _, shingles):
        for shingle in shingles:
            yield shingle, 1

    # Combines all the 1 from third_mapper and yields the shingle and number of occurrences
    # Yields the values as one variable to make it easier to sort later
    def third_reducer(self, key, values):
        yield None, (sum(values), key)
        # yield key, sum(values)

    # Yields the n most occurring shingles
    def sort_reducer(self, _, shingles):
        so = sorted(shingles, reverse=True)
        for val in so[:50]:
            yield val

if __name__ == '__main__':
    MRPrepro.run()
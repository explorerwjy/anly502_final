import mrjob
import mrjob.compat
from mrjob.job import MRJob
from mrjob.step import MRStep

# import re
from mrjob.protocol import JSONValueProtocol

class businessReviewJoin(MRJob):


    INPUT_PROTOCOL = JSONValueProtocol
    SORT_VALUES=True
    
    def mapper(self, _, data):
        if data['type'] == 'review':
            self.increment_counter('status','review.json found',1)
            try:
                yield data['business_id'], ('review', data['text'])
            except ValueError as e:
                pass
        elif data['type'] == 'business':
            self.increment_counter('status','business.json found',1)

            try:
                yield data['business_id'], ('categories',  data['categories'])
            except ValueError :
                return


    def reducer(self, key, values):
        cate = None
        for v in values:
            if  v[0]=='categories':
                cate = v[1]
                continue
            if not cate:
                self.increment_counter('Warning','No Cate Found',1)
                continue
            self.increment_counter('Status','Cate Found',1)
            yield cate, v[1]
            
            
    def tally_mapper (self, key, values):
        for item in key:
            yield item, len(values)
    def tally_combiner(self, key, values):
        count = 0
        total = 0
        for value in values:
            count += 1
            total += value
        yield key, float(total/count)        

    def tally_reducer(self, key, values):
        count = 0
        total = 0
        for value in values:
            count += 1
            total += value
        yield key, float(total/count)        
        
        
    def steps(self):
        return[
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
                   
            MRStep(mapper = self.tally_mapper,
                   combiner = self.tally_combiner,
                   reducer = self.tally_reducer)]
                   
if __name__ == '__main__':
    businessReviewJoin.run()




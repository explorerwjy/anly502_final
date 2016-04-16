import mrjob
import mrjob.compat
from mrjob.job import MRJob
from mrjob.step import MRStep

import re
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
                yield data['business_id'], ('city',  data['city'])
            except ValueError :
                return


    def reducer(self, key, values):
        city = None
        for v in values:
            if  v[0]=='city':
                city = v[1]
                continue
            if not city:
                self.increment_counter('Warning','No City Found',1)
                continue
            self.increment_counter('Status','City Found',1)
            yield city, v[1:]
            
            
    def tally_mapper (self, key, values):
        yield key , (1,len(values))
    def tally_combiner(self, key, values):
        yield key, (1,float(sum(values[1])/sum(values[0])))
        
    def tally_reducer(self, key, values):
        yield key, float(sum(values[1])/sum(values[0]))
        
        
    def steps(self):
        return[
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
                   
            MRStep(mapper = self.tally_mapper,
                   combiner = self.tally_combiner,
                   reducer = self.tally_reducer)]
                   
if __name__ == '__main__':
    businessReviewJoin.run()

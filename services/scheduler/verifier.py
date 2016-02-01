import json
import time, dateutil.parser
from UNISrt import UNISrt

class Verifier(object):
    '''
    test if the scheduled result is correct
    '''
    def __init__(self, unisrt):
        self.unisrt = unisrt
    
    def verify(self):
        ledger = {}
        for m in filter(lambda x: x.scheduled_times, self.unisrt.measurements['existing'].values()):
            for r in m.resources:
                ledger.setdefault(r, []).extend(m.scheduled_times)
                
        # a naive (also SLOW) way to examine all the scheduled times. Just meant to be a
        # different algorithm from the one we use to schedule them at the first place
        for times in ledger.itervalues():
            for key, time in enumerate(times):
                for another_time in times[key + 1:]:
                    if dateutil.parser.parse(time['start']) < dateutil.parser.parse(another_time['start']):
                        assert dateutil.parser.parse(time['end']) <= dateutil.parser.parse(another_time['start'])
                    elif dateutil.parser.parse(time['start']) > dateutil.parser.parse(another_time['start']):
                        assert dateutil.parser.parse(time['start']) >= dateutil.parser.parse(another_time['end'])
                    else:
                        assert 1 == 0
                        
def main():    
    unisrt = UNISrt.UNISrt()
    verifier = Verifier(unisrt)
    unisrt.register(verifier.verify)
    unisrt.run(-1)

if __name__ == '__main__':
    main()
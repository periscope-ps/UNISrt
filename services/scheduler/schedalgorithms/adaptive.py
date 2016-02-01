import re

def build_basic_schedule(start, td_every, td_duration, num_to_schedule, conflicts):
    '''
    build schedule, avoiding all conflicting time slots
    '''
    schedule = []
    cur = start
    for t in conflicts:
        
        while (t["start"] - cur) > td_duration:
            schedule.append({"start":datetime_to_dtstring(cur),
                             "end":datetime_to_dtstring(cur+td_duration)})
            cur += td_every
            if len(schedule) >= num_to_schedule:
                return schedule
        
        if cur <= t["end"]:
            cur = t["end"]
            
    # finish building schedule if there are no more conflicts
    while len(schedule) < num_to_schedule:
        s = datetime_to_dtstring(cur)
        e = datetime_to_dtstring(cur + td_duration)
        schedule.append({"start":s, "end":e})
        cur += td_every
            
    return schedule
    
def datetime_to_dtstring(dt):
    '''
    convert datetime object to a date-time string that UNIS will accept
    '''
    st = dt.isoformat()
    st = st[:st.index('+')]
    st += 'Z'
    st = re.sub("\.[0-9]+", "", st)
    return st
#/usr/bin/env python3

# This code tries to maintain compatibility with python2
from itertools import islice
import re
# have separate sets for origins with small frequency
from collections import defaultdict
import heapq
import sys
from sortedcontainers import SortedDict
from datetime import datetime
# This script is both python2 and python3 compatible
import sys

commandline_args =  sys.argv

is_python3 = (sys.version_info > (3, 0))

try:
    from itertools import izip
except ImportError as e:
    pass

if is_python3:
    globals()['xrange'] = range
    globals()['izip']=zip

from py2_utils import FixedOffSet
def parse_timestring(ts):
    if is_python3:
        return datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")
    date_str, _, tzone = ts.rpartition(' ')
    dt = datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S")
    offset = int(tzone[-4:-2])*60 + int(tzone[-2:])
    if tzone[0]=='-':
        offset = -offset
    return dt.replace(tzinfo=FixedOffSet(offset))

from sortedcontainers import *
from collections import Counter
class FastCounter: # separate storage for low frequency keys
    def __init__(self, number_of_low_freq_hash=0, top_n=10):
        self.number_of_low_freq_hash=number_of_low_freq_hash
        self.seen_k_th = SortedList() # [set() for _ in xrange(number_of_low_freq_hash)]
        self.main_origin_table = {}
        self.top_n = top_n
    
    def __iter__(self): # 
        top10 = [] # accessible from this scope
        top_n = self.top_n
        if is_python3:
            top10 = list(heapq.nlargest(top_n, self.main_origin_table.items(), key=lambda x: x[1]))
        else:
            top10 = list(heapq.nlargest(top_n, self.main_origin_table.iteritems(), key=lambda x: x[1]))
        
        for tup in top10:
            yield tup
        
        c = Counter(self.seen_k_th)
        for it,_ in top10:
            del c[it]
        yield from c.most_common(top_n)
        
    def incr(self,it, step_=1):
        if it in self.main_origin_table:
            self.main_origin_table[it]+=step_
            return
        k = server_stats.number_of_low_freq_hash
        count_=self.seen_k_th.count(it)+step_
        if count_>k:
            self.main_origin_table[it]=count_
            # the value is left in the low frequency storage, sacrifising memory for speed
            # the idea is that the items in the seen_k_th table will likely not make it to the top 10 list
        else:
            self.seen_k_th.extend(repeat(it,step_))
        
class server_stats:
    number_of_low_freq_hash = 5 # a reasonable value for this parameter
    def __init__(self):
        self.seen_k_th = [set() for _ in xrange(server_stats.number_of_low_freq_hash)]
        self.main_origin_table = {}
        self.res_consumption = FastCounter()
        self.temporal_stats = SortedDict()
    
    def add_time_info_from_string(self, st):
        dt = parse_timestring(st)
        if dt in self.temporal_stats:
            self.temporal_stats[dt]+=1
        else:
            self.temporal_stats[dt]=1
        #self.temporal_stats
    
    def add_resource_consumption(self, it, int_val):
        self.res_consumption.incr(it,int_val)
    
    def top10_res(self):
        return islice(self.res_consumption, 10)
    
    def incr(self,it):
        if it in self.main_origin_table:
            self.main_origin_table[it]+=1
        k = server_stats.number_of_low_freq_hash
        if it in self.seen_k_th[k-1]:
            self.seen_k_th[k-1].remove(it)
            self.main_origin_table[it]=k+1
            return
        for i in xrange(1,k):
            if it in self.seen_k_th[i-1]:
                self.seen_k_th[i-1].remove(it)
                self.seen_k_th[i].add(it)
                return
        self.seen_k_th[0].add(it) # `seen once` goes to 0 index

    def __str__(self):
        from itertools import chain,islice
        k = server_stats.number_of_low_freq_hash
        top10 = [] # accessible from this scope
        if is_python3:
            top10 = [[origin,",",str(v),"\n"]
                                   for origin,v in heapq.nlargest(10, self.main_origin_table.items(), 
                                   key=lambda x: x[-1])]
        else:
            top10 = [[origin,",",str(v),"\n"]
                                   for origin,v in heapq.nlargest(10, self.main_origin_table.iteritems(), 
                                   key=lambda x: x[-1])]
        top_freq = server_stats.number_of_low_freq_hash
        while len(top10)<10 and top_freq>=1:
            top10 += [[origin,",",str(top_freq),"\n"] for origin in self.seen_k_th[top_freq-1]]
            top_freq-=1
        return "".join(chain.from_iterable(islice(top10, 10)))

hist = server_stats()

from datetime import timedelta
from collections import deque
import itertools

def compute_1hr_count(ordered_dict, start_time):
    if is_python3:
        return deque(itertools.accumulate(ordered_dict[k] for k in ordered_dict.irange(start_time, start_time+timedelta(hours=1))), maxlen=1)[0]
    freq = 0;
    for k in ordered_dict.irange(start_time, start_time+timedelta(hours=1)):
        freq+=ordered_dict[k]
    return freq

class SlidingWindowCount:
    def __init__(self, temporal_stats, time_frame = timedelta(hours=1)):
        self.temporal_stats = temporal_stats
        self.old_sum = compute_1hr_count(hist.temporal_stats, min(hist.temporal_stats))
        self.time_frame = time_frame
        self.start_time = min(hist.temporal_stats)
        
    def __iter__(self):
        ordered_dict = self.temporal_stats
        while self.start_time < max(ordered_dict):
            yield (self.start_time, self.old_sum)
            self.old_sum=sum([self.old_sum]+list(-ordered_dict[k] for k in ordered_dict.irange(self.start_time,self.start_time))
        + list(ordered_dict[k] for k in ordered_dict.irange(self.start_time+self.time_frame,self.start_time+self.time_frame)))
            self.start_time += timedelta(seconds=1)


from itertools import repeat, starmap
class ThreeStrikeCounter:
    def __init__(self, logwriter, time_frame=timedelta(seconds=20)):
        self.storage = defauldict(lambda: deque(maxlen=3))  # ring buffer data structure
        self.time_frame=time_frame
        self.banned_starts = []

    def add(self, ipaddr, new_time, end_point):
        
        bts = [t for (id_, t) in self.banned_starts if id_==ipaddr]
        if len(bts)>0:
            if (max(bts)+timedelta(minutes=5))>new_time:
                return True
        
        if end_point.startswith('/login'):
            err401_times = self.storage[ipaddr]
            err401_times.push(new_time)
            min_start_time = new_time-self.time_frame
            if (err401_times[0] > min_start_time) and len(err401_times)==3:
                self.banned_starts.append((ipaddr, new_time))
            return False
    
    def reset(self, ipaddr):
        self.storage[ipaddr].clear()

time2str = lambda dt: datetime.strftime(dt,"%d/%b/%Y:%H:%M:%S %z")

import re
#ip_regex_whost_check = re.compile(r'^((?P<ipaddr>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})|(?P<domain_name>([a-z0-9\-\_]+\.)+[a-z0-9]+)|(?P<local_host_name>[a-z0-9\-\_]+))\s-\s-\s\[(?P<time_stamp>.*?)\].*?\s(?P<bytes>\d+)?$')
ip_regex = re.compile(r'^(?P<ipaddr>.*?)\s-\s-\s\[(?P<time_stamp>.*?)\]\s\"[A-Z]+?\s(?P<res>.*?)\s.*?\".*?\s(?P<bytes>\d+)?$')

from itertools import islice
hist = server_stats()

kwargs = {}
if is_python3:
    kwargs['encoding']="latin-1"

with open(commandline_args[1], 'r', **kwargs) as f:
    for line in f:
        mat = ip_regex.match(line)
        if mat:
            origin_dict = mat.groupdict()
            origin_id = origin_dict['ipaddr']
            hist.incr(origin_id)
            if origin_dict['bytes']:
                hist.add_resource_consumption(origin_dict['res'].split(" ")[0], int(origin_dict['bytes']))
            hist.add_time_info_from_string(origin_dict['time_stamp'])
        else:
            print('Malformed line -->', line)
    
    with open(commandline_args[2], 'w') as hosts_file:
        hosts_file.write(str(hist))
    
    with open(commandline_args[4], 'w') as res_file:
        for origin,_ in hist.top10_res():
            res_file.write(origin)
            res_file.write("\n")
            
    sc = SlidingWindowCount(hist.temporal_stats)
    from itertools import chain
    with open(commandline_args[3], 'w') as hour_file:
        #hour_file.write("\n".join(["".join([time2str(ti),",",str(v)]) for v, ti in heapq.nlargest(10, [(sc.rollover_one(t1,t2),t1) for (t1, t2) in izip(chain([min(hist.temporal_stats)], hist.temporal_stats), hist.temporal_stats)], key=lambda x:x[0])]))
        hour_file.write("\n".join(["".join([time2str(ti),",",str(v)]) for ti,v in heapq.nlargest(10, sc, key=lambda x:x[1])]))
    with open(commandline_args[5], 'w') as blocked_file:
        pass


# EXTRA FEATURES
# which origin gave the most error messages table sorted by error code
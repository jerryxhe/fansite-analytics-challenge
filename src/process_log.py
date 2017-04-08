#/usr/bin/env python3
# This script is both python2 and python3 compatible
from itertools import islice
import re
from collections import defaultdict
import heapq
import sys
from sortedcontainers import SortedDict
from datetime import datetime

import sys
commandline_args =  sys.argv

from sortedcontainers import *
from itertools import repeat
from collections import Counter

try:
    from itertools import izip
except ImportError as e:
    pass

is_python3 = (sys.version_info > (3, 0))


if is_python3:
    globals()['xrange'] = range
    globals()['izip']=zip


class server_stats: 
    """a class to keep track of summary statistics"""
    number_of_low_freq_hash = 5 # a reasonable value for this parameter
    def __init__(self):
        self.seen_k_th = [set() for _ in xrange(server_stats.number_of_low_freq_hash)]
        self.main_origin_table = {}
        self.res_consumption = defaultdict(int)
        self.temporal_stats = SortedDict()

    def add_time_info_from_string(self, st):
        dt = parse_timestring(st)
        if dt in self.temporal_stats:
            self.temporal_stats[dt]+=1
        else:
            self.temporal_stats[dt]=1
        #self.temporal_stats
    
    def top10_res(self):
        if is_python3:
            return heapq.nlargest(10, self.res_consumption.items(), 
                                   key=lambda x: x[-1])
        return heapq.nlargest(10, self.res_consumption.iteritems(), 
                                   key=lambda x: x[-1])
                                   
    def add_resource_consumption(self, it, int_val):
        self.res_consumption[it]+=int_val
    
    def incr(self,it):
        if it in self.main_origin_table:
            self.main_origin_table[it]+=1
        k = server_stats.number_of_low_freq_hash
        if it in self.seen_k_th[k-1]:
            self.seen_k_th[k-1].remove(it)
            self.main_origin_table[it]=k+1
            return
        for i in range(1,k):
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

from common_tools import *

class ThreeStrikeCounter:
    def __init__(self, time_frame=timedelta(seconds=20)):
        self.storage = defaultdict(lambda: deque(maxlen=3))  # ring buffer data structure
        self.time_frame=time_frame
        self.banned_starts = []

    def add(self, ipaddr, new_time, end_point):
        for (id_, t) in reversed(self.banned_starts):
            if id_==ipaddr:
                if (t+timedelta(minutes=5))>new_time:
                    return True
            if t < (new_time -timedelta(minutes=5)):
                break
        if end_point.startswith('/login'):
            err401_times = self.storage[ipaddr]
            err401_times.append(new_time)
            min_start_time = new_time-self.time_frame
            if (err401_times[0] > min_start_time) and len(err401_times)==3:
                self.banned_starts.append((ipaddr, new_time))
            return False
    
    def reset(self, ipaddr):
        self.storage[ipaddr].clear()

time2str = lambda dt: datetime.strftime(dt,"%d/%b/%Y:%H:%M:%S %z")
from itertools import islice
hist = server_stats()

kwargs = {}
if is_python3:
    kwargs['encoding']="latin-1"

err_table = defaultdict(int)

blocked_file = open(commandline_args[5], 'w')
tsc = ThreeStrikeCounter()
with open(commandline_args[1], 'r', **kwargs) as f:
    for origin_dict,line in LogParser(f):
        origin_id = origin_dict['ipaddr']
        hist.incr(origin_id)
        if origin_dict['bytes']:
            hist.add_resource_consumption(origin_dict['res'].split(" ")[0], int(origin_dict['bytes']))
        hist.add_time_info_from_string(origin_dict['time_stamp'])
        res = origin_dict['res']
        if res.startswith('/login'):
            if origin_dict['code']=='200':
                tsc.reset(origin_id)
            elif origin_dict['code']=='401':
                if tsc.add(origin_id, parse_timestring(origin_dict['time_stamp']),res):
                    blocked_file.write(line)
                    
        if int(origin_dict['code'])/100==4: # an error has occurred
            err_table[origin_id]+=1
    
with open(commandline_args[2], 'w') as hosts_file:
    hosts_file.write(str(hist))

with open(commandline_args[4], 'w') as res_file:
    for origin,_ in hist.top10_res():
        res_file.write(origin)
        res_file.write("\n")
   
sc = SlidingWindowCount(hist.temporal_stats)
from itertools import chain
with open(commandline_args[3], 'w') as hour_file:
    ans = sc.iterative_search()
    hour_file.write("\n".join(["".join([time2str(ti),",",str(v)]) for ti,v in ans]))
    hour_file.write("\n")

# EXTRA FEATURES
# which origin gave the most error messages table sorted by error code        
with open(commandline_args[5], 'w') as err_agents_file:
    top10 = []
    if is_python3:
        top10 = heapq.nlargest(10, err_table.items(), 
                               key=lambda x: x[-1])
    else:
        top10 = heapq.nlargest(10, err_table.iteritems(), 
                               key=lambda x: x[-1])
    for (origin_id, count_) in top10:
        err_agents_file.write(",".join([origin_id,str(count_)]))
        err_agents_file.write("\n")

blocked_file.close()
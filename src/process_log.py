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

from datetime import timedelta, tzinfo

class FixedOffSet(tzinfo): # I attribute this class to http://stackoverflow.com/questions/1101508/how-to-parse-dates-with-0400-timezone-string-in-python
    """Fixed offset in minutes: `time = utc_time + utc_offset`."""
    def __init__(self, offset):
        self.__offset = timedelta(minutes=offset)
        hours, minutes = divmod(offset, 60)
        #NOTE: the last part is to remind about deprecated POSIX GMT+h timezones
        #  that have the opposite sign in the name;
        #  the corresponding numeric value is not used e.g., no minutes
        self.__name = '<%+03d%02d>%+d' % (hours, minutes, -hours)
    def utcoffset(self, dt=None):
        return self.__offset
    def tzname(self, dt=None):
        return self.__name
    def dst(self, dt=None):
        return timedelta(0)
    def __repr__(self):
        return 'FixedOffset(%d)' % (self.utcoffset().total_seconds() / 60)

def parse_timestring(ts):
    if is_python3:
        return datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")
    date_str, _, tzone = ts.rpartition(' ')
    dt = datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S")
    offset = int(tzone[-4:-2])*60 + int(tzone[-2:])
    if tzone[0]=='-':
        offset = -offset
    return dt.replace(tzinfo=FixedOffSet(offset))



class server_stats:
    number_of_low_freq_hash = 20 # a reasonable value for this parameter
    def __init__(self):
        self.seen_k_th = [set() for _ in xrange(server_stats.number_of_low_freq_hash)]
        self.main_origin_table = {}
        self.res_consumption = defaultdict(int)
        self.temporal_stats = SortedDict()
        self.last_seen_time = None # to identify duplicates
    
    def add_time_info_from_string(self, st):
        dt = parse_timestring(st)
        if dt in self.temporal_stats:
            self.temporal_stats[dt]+=1
        else:
            self.temporal_stats[dt]=1
        #self.temporal_stats
    
    def add_resource_consumption(self, it, int_val):
        self.res_consumption[it]+=int_val
    
    def top10_res(self):
        if is_python3:
            return heapq.nlargest(10, self.res_consumption.items(), 
                                   key=lambda x: x[-1])
        return heapq.nlargest(10, self.res_consumption.iteritems(), 
                                   key=lambda x: x[-1])
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
        if is_python3:
            return "".join(chain.from_iterable([origin,",",str(v),"\n"]
                                   for origin,v in heapq.nlargest(10, self.main_origin_table.items(), 
                                   key=lambda x: x[-1])))
        return "".join(chain.from_iterable([origin,",",str(v),"\n"]
                                   for origin,v in heapq.nlargest(10, self.main_origin_table.iteritems(), 
                                   key=lambda x: x[-1])))


hist = server_stats()


from datetime import timedelta
from collections import deque
import itertools
import operator

def compute_1hr_freq(ordered_dict, start_time):
    if is_python3:
        return deque(itertools.accumulate(ordered_dict[k] for k in ordered_dict.irange(start_time, start_time+timedelta(hours=1))), maxlen=1)[0]
    freq = 0;
    for k in ordered_dict.irange(start_time, start_time+timedelta(hours=1)):
        freq+=ordered_dict[k]
    return freq

def compute_1hr_freq(ordered_dict, start_time):
    if is_python3:
        return deque(itertools.accumulate(ordered_dict[k] for k in ordered_dict.irange(start_time, start_time+timedelta(hours=1))), maxlen=1)[0]
    freq = 0;
    for k in ordered_dict.irange(start_time, start_time+timedelta(hours=1)):
        freq+=ordered_dict[k]
    return freq

class StreamingCount:
    def __init__(self, temporal_stats, time_frame = timedelta(hours=1)):
        self.temporal_stats = temporal_stats
        self.old_sum = compute_1hr_freq(hist.temporal_stats, min(hist.temporal_stats))
        self.time_frame = time_frame

    def rollover_one(self, old_start_time, new_start_time):
        old_sum = self.old_sum
        time_frame = self.time_frame
        ordered_dict = self.temporal_stats
        if is_python3:
            self.old_sum = sum([old_sum-ordered_dict[old_start_time]]
                       +list(deque(itertools.accumulate(ordered_dict[k] for k in ordered_dict.irange(old_start_time+time_frame, new_start_time+time_frame)), maxlen=1)))
            return self.old_sum
        old_sum -= ordered_dict[old_start_time]
        for k in ordered_dict.irange(old_start_time+time_frame,new_start_time+time_frame):
            old_sum += ordered_dict[k]
        self.old_sum = old_sum
        return old_sum

from itertools import repeat, starmap
class ExpiringCounter:
    def __init__(self, logwriter, time_frame=timedelta(minutes=3)):
        self.storage = defauldict(lambda: deque(maxlen=3))
        self.time_frame=time_frame

    def add(self, ipaddr, new_time):
        err401_times = self.storage[ipaddr]
        err401_times.push(new_time)
        min_start_time = new_time-self.time_frame
        list(starmap(err401_times.popleft, repeat((), sum([1 for t0 in islice(err401_times,2) if t0 < min_start_time]))))
        
        if len(err401_times)==3:
            logwriter.write('hi')

time2str = lambda dt: datetime.strftime(dt,"%d/%b/%Y:%H:%M:%S %z")

import re
#ip_regex_whost_check = re.compile(r'^((?P<ipaddr>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})|(?P<domain_name>([a-z0-9\-\_]+\.)+[a-z0-9]+)|(?P<local_host_name>[a-z0-9\-\_]+))\s-\s-\s\[(?P<time_stamp>.*?)\].*?\s(?P<bytes>\d+)?$')
ip_regex = re.compile(r'^(?P<ipaddr>.*?)\s-\s-\s\[(?P<time_stamp>.*?)\]\s\"[A-Z]+?\s(?P<res>.*?)\".*?\s(?P<bytes>\d+)?$')

from itertools import islice
hist = server_stats()
with open(commandline_args[1], 'r', encoding="latin-1") as f:
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
            
    sc = StreamingCount(hist.temporal_stats)
    with open(commandline_args[3], 'w') as hour_file:
        for v, ti in heapq.nlargest(10, [(sc.rollover_one(t1,t2),t1) for (t1, t2) in izip(hist.temporal_stats, hist.temporal_stats.irange(min(hist.temporal_stats)+timedelta(seconds=1)))], key=lambda x:x[0]):
            hour_file.write(time2str(ti)+","+str(v)+"\n")

    with open(commandline_args[5], 'w') as blocked_file:
        pass

# EXTRA FEATURES
# which origin gave the most error messages table sorted by error code
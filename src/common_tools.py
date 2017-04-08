#!/usr/bin/env python3
from datetime import timedelta,datetime
from collections import deque,Counter,defaultdict
import itertools
import sys
is_python3 = (sys.version_info > (3, 0))
import heapq

try:
    from itertools import izip
except ImportError as e:
    pass

if is_python3:
    globals()['xrange'] = range
    globals()['izip']=zip

import re
class LogParser:
    def __init__(self, line_iterable):
        self.ip_regex = re.compile(r'^(?P<ipaddr>.*?)\s-\s-\s\[(?P<time_stamp>.*?)\]\s"([A-Z]+?\W)?(?P<res>.*)\s?.*?\"\s?(?P<code>\d{3})\s?(?P<bytes>\d+)?.*?$')
        self.f = line_iterable
    
    def __iter__(self):
        for line in self.f:
            mat = self.ip_regex.match(line)
            if mat:
                yield mat.groupdict(),line
            else:
                print('Malformed line -->', line)

from py2_compat_utils import FixedOffSet

def parse_timestring(ts):
    if is_python3:
        return datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")
    date_str, _, tzone = ts.rpartition(' ')
    dt = datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S")
    offset = int(tzone[-4:-2])*60 + int(tzone[-2:])
    if tzone[0]=='-':
        offset = -offset
    return dt.replace(tzinfo=FixedOffSet(offset))

def sum_consecutive(x):
    if len(x)<=1:
        return x
    else:
        return [sum(x[i:i+2]) for i in range(len(x)-1)]

def time_range(st, fin, incr=timedelta(seconds=1)):
    """a version of built-in range that works with time"""
    if st <= fin:
        while st < fin:
            yield st
            st += incr
    else:
        while st > fin:
            yield st
            st += incr

def compute_count_in_range(temporal_stats, start_time, end_time):
    """finds the number of log entries in a given time range the brute force way"""
    ordered_dict = temporal_stats 
    if is_python3:
        return (start_time, end_time, deque(itertools.accumulate(ordered_dict[k] for k in ordered_dict.irange(start_time, end_time)), maxlen=1)[0])
    freq = 0;
    for k in ordered_dict.irange(start_time, end_time):
        freq+=ordered_dict[k]
    return (start_time, end_time, freq)

class WindowCountIterator:
    """an iterator class to iterate over all sliding 1 hour window sums in a time interval (start_time, end_time)"""
    def __init__(self, temporal_stats, start_time, end_time,time_frame=timedelta(hours=1)):
        self.start_time = start_time
        self.end_time = end_time
        self.temporal_stats = Counter(temporal_stats) # # unlike defaultdict, Counter doesn't add a key on a failed query
        _,_,self.old_sum = compute_count_in_range(temporal_stats, start_time, end_time)
        self.time_frame=time_frame
        
    def reset(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        return self
        
    def __iter__(self):
        ordered_dict=self.temporal_stats
        while self.start_time < self.end_time:
            yield (self.start_time, self.old_sum)
            self.old_sum=self.old_sum-ordered_dict[self.start_time]
            if self.end_time > self.start_time + self.time_frame:
                self.old_sum+=ordered_dict[self.start_time + self.time_frame]
            self.start_time += timedelta(seconds=1)


from itertools import chain,starmap
import operator
from functools import partial 

class SlidingWindowCount:
    """gets the best 10 of the sliding 1 hour windows within all times"""
    def __init__(self, temporal_stats, time_frame = timedelta(hours=1)):
        self.temporal_stats = temporal_stats
        max_time = max(temporal_stats)
        min_time = max(temporal_stats)
        timeline = time_range(min_time,max_time, time_frame)
        self.timeline = list(chain(timeline, [max_time])) 
        if len(self.timeline)==1:
            timeline = [min(temporal_stats)]
        self.compute_count_in_range = partial(compute_count_in_range, self.temporal_stats)
        self.windows_orig = list(starmap(self.compute_count_in_range, izip(timeline, self.timeline)))
        
    def iterative_search(self):
        wc = []
        initial10 = [(datetime.now(), 0)]
        if len(self.timeline)==1:
            wc = WindowCountIterator(self.temporal_stats, min(self.temporal_stats), max(self.temporal_stats))
            initial10 = heapq.nlargest(10, chain(wc,initial10), key=lambda x:x[1])
        else:
            consecutive_sums = list(enumerate(sum_consecutive([v for (_, _, v) in self.windows_orig])))
            wc = WindowCountIterator(self.temporal_stats, self.timeline[i], self.timeline[i+1])
            for i, count_ in sorted(consecutive_sums, key=lambda x: x[1]):
                if count_ < initial10[-1][1]:
                    break
                wc.reset(self.timeline[i], self.timeline[i+1])
                initial10 = heapq.nlargest(10, chain(wc,initial10), key=lambda x:x[1])
        return list(initial10)

from bisect import bisect_left, bisect_right
class SortedTop10:
    def __init__(self, key=lambda x: -x[0]):
        self._key=key
        self._keys = [0]
        self._values = [(0,datetime.now(), datetime.now())]
       
    def trim(self):
        self._keys=self._keys[:10]
        self._values = self._values[:10]
    
        
    def insert(self, tup):
        k = self._key(tup)
        i = bisect_left(self._keys, k)
        self._keys.insert(i, k)
        self._values.insert(i, tup)
        if i>10:
            self.trim()
        
    def __iter__(self):
        for v in self._values[:10]:
            yield v

from array import array
class TimeIndexCumSum:
    def __init__(self):
        self.timeline = [None]*100000
        self.cumsum = array('L', [0])
        self.count = 0
        self.i=-1
        self.top10 = SortedTop10()
        self.j=0;   # start of the 1 hour window
        self.mint = None
    
    def add(self, t):
        i = self.i
        if i+3 > len(self.timeline):
            self.timeline += [None]*10000
        if t!=self.timeline[i]:
            self.i+=1
            self.timeline[self.i]=t 
            self.cumsum.append(self.cumsum[i+1]+1)
            while self.j < self.i and (t - self.timeline[self.j]) > timedelta(hours=1):
                self.j+=1
                if self.j < self.i and ((t - self.timeline[self.j]) < timedelta(hours=1)):
                    self.top10.insert((self.cumsum[self.i+1]-self.cumsum[self.j-1], self.timeline[self.j-2], self.timeline[self.i]))
                    break
        else:
            self.cumsum[i+1]+=1
        if self.mint is None:
            self.mint = t
        self.maxt = t
        
    def pad_end(self):
        if (self.maxt - self.mint) < timedelta(hours=1):
            final_count=self.cumsum[self.i+1]
            firstpass = False
            for t in time_range(self.maxt+timedelta(seconds=1), self.maxt+timedelta(hours=1)):
                while self.j < self.i and (t - self.timeline[self.j]) > timedelta(hours=1):
                    self.j+=1
                    firstpass = True
                if self.j <= self.i and firstpass:
                    self.top10.insert((final_count-self.cumsum[self.j-1], self.timeline[self.j-1], t))

            
    def __str__(self):
        return repr(self.top10._values)
        
class ThreeStrikeCounter: 
    """for blocked.txt"""
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

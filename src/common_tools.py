#!/usr/bin/env python3
from datetime import timedelta,datetime
from collections import deque,Counter
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
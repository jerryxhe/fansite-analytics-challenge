# Implementation

This script requires the sortedcontainers==1.5.7 python package, please install by running install.sh or just use pip or pip3. 
The original design is to use Python3, but later I made it Python2 compatible as well for it to be able to port to more potential environments

### Feature 1 
List in descending order the top 10 most active hosts/IP addresses that have accessed the site.
I used a hash table for frequently occurring hosts and a 5 individual python sets to store infrequent ones 

### Feature 2 
Identify the top 10 resources on the site that consume the most bandwidth. Bandwidth consumption can be extrapolated from bytes sent over the network and the frequency by which they were accessed.

I used a simple defaultdict counter

### Feature 3 
List in descending order the site’s 10 busiest (i.e. most frequently visited) 60-minute period.

I first looked all non-overlapping 60 minute intervals, sorted them  by the count (descending) in each interval. I iterated every second of the pair of consecutive intervals with the highest sum count. Use that to get the initial top 10 60 minute periods. I still have to iterate over other consecutive 60 minute pairs, but now I can logically drop any pair whose sum count is less than the lowest 60-minute count of the current top 10. This results in significant speedup over the naive approach. 

I have also started devising an even faster way to do this by storing the cumulative count indexed by time, see class TimeIndexCumSum in and class SortedTop10 in common_tools.py. The idea is that we can compute the total count in any interval by subtractuing cumulative count of the respective time endpoint; however, I could not complete this implementation on time. I leave the code in for your reference. 

### Feature 4 
Your final task is to detect patterns of three consecutive failed login attempts over 20 seconds in order to block all further attempts to reach the site from the same IP address for the next 5 minutes. Each attempt that would have been blocked should be written to a log file named `blocked.txt`.

### Feature 5 (Additional)
I thought it would be useful to know which ip addresses are causing the most error. I define error as any HTTP code in the 400s. I list them in the format `ip,frequency` in descending order.





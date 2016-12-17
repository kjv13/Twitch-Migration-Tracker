import time
from db_connect import NoSQLConnection

print('connection to noSQL database')
con = NoSQLConnection()

# Constants
# the frequency db should be checked in seconds
check_freq = 300
# the limit for joining and leaving entries to be considered related
related_limit = 300

# get list of streams being recorded in watching collection
streams = con.db[con.watching_collection].find(
        {},
        {
            '_id': False,
            'streamname': True,
            'joining': True,
            'leaving': True
        }
)

# this creates a tuple of tuples
# first element is streamname
# second element is a list of joining users
# third element is a list of leaving elements
# joining and leaving lists contain tuples themselves
# first element of joining/leaving tuples is the username
# second element is the last_updated time
# ('streamname',
# [('username','last_updated'),...],
# [('username','last_updated'),...])
streams = list(map(lambda s: (s['streamname'],
                              list(map(lambda j: (j['username'],
                                                  j['last_updated']),
                                       s['joining'])),
                              list(map(lambda l: (l['username'],
                                                  l['last_updated']),
                                       s['leaving']))),
                   streams))

# for every pair of streams
for i in streams:
    for j in streams:
        # get the list of users leaving i
        leaving_i = i[2]
        # list of users joining j
        joining_j = j[1]
        # for every pair of leaving and joining users
        for l in leaving_i:
            for j in joining_j:
                # if the leaving user and joining user has the same username
                if l[0] == j[0]:
                    # find the difference and time and see if its less than
                    # limit
                    if max(l[1], j[1]) - min(l[1], j[1]) <= related_limit:
                        # this is considered a connection so add it to db
                        pass

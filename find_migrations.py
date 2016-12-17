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
# streams = list(map(lambda s: (s['streamname'],
#                               (list(map(lambda j: (j['username'],
#                                                   j['last_updated']),
#                                        s['joining'])),
#                               list(map(lambda l: (l['username'],
#                                                   l['last_updated']),
#                                        s['leaving']))),
#                    streams))

# this should create a tuple of the form ...
# ('streamname', {'username': 'last_updated',...},
# {'username': 'last_updated',...})
# where the first value is a single string representing the stream name
# the second value is a dict representing users joining
# and the third value is a dict representing users leaving
# the dict is of the form ...
# {
#  'username': 'last_updated',
#  ...
# }
streams = list(map(lambda s: (s['streamname'],
                              dict(zip(list(map(lambda j: j['username'],
                                                s['joining'])),
                                       list(map(lambda j: j['last_updated'],
                                                s['joining'])))),
                              dict(zip(list(map(lambda l: l['username'],
                                                s['leaving'])),
                                       list(map(lambda l: l['last_updated'],
                                                s['leaving']))))),
                   streams))

# for every pair of streams
for l in streams:
    for j in streams:
        # if i and j are the same stream we don't care about leaving and
        # joining
        if l[0] == j[0]:
            continue
        # for every user that joined i and left j
        # user = the strings of the keys that are found in i and j
        # where the keys are the usernames of users who leave/join
        for user in set(l[2].keys() & j[1].keys()):
            # find the difference and time and see if its less than
            # limit
            # i[user] gets the value for the key user, which is the
            # last_updated time
            if (max(l[2][user], j[1][user]) -
                    min(l[2][user], j[1][user]) <= related_limit):
                # this is considered a connection so add it to db
                print(('user {0} left stream {1} and went to stream ' +
                      ' {2}').format(user, l[0], j[0]))

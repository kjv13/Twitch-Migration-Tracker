# import pdb
import threading
from irc_connect import IRCConnection
from api_connect import APIConnection
from db_connect import NoSQLConnection
from stream import Stream

print('connecting to noSQL database')
con = NoSQLConnection()

print('connecting to IRC server')
irc = IRCConnection()

print('creating API connection')
api = APIConnection()

# Constants
#  the time to live for the joining user elements in database
join_ttl = 600
#  this is the number of seconds before leave elements are
#  removed
leave_ttl = 600

#  if the number of users from IRC is less then 100 then
# the result is double checked with an API call
irc_min_users = 100

# the limit for joining and leaving entries to be considered related
related_limit = 300

print_lock = threading.RLock()
irc_lock = threading.RLock()
api_lock = threading.RLock()

watching = []


def main():
    global watching

    while True:
        print("""

        -------------------------------------------------------
        streams updated now starting from beginning
        -------------------------------------------------------

        """)

        streams = get_monitored_streams()
        streams = [stream['streamname'] for stream in streams[0]['streams']]

        # eliminate any Streams in watching where stream.name is not in streams
        watching = list(filter(lambda s: s.name in streams, watching))
        # add any streams in streams but not in watching
        # by creating a new Stream object
        for stream in streams:
            if stream not in list(map(lambda s: s.name, watching)):
                watching.append(Stream(stream))

        # at this point the streams in watching should be the same streams in
        # streams

        threads = []
        print('\n\n{}\n\n'.format(threading.active_count()))
        for stream in streams:
            t = threading.Thread(target=update_stream, name=stream+'-thread',
                                 args=(stream,))
            print('{} threads created'.format(len(streams)))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
            print('\n\nthread joined back into main thread\n\n')
        print('all threads joined back into main thread')

        print('now uploading to db')
        for l in watching:
            con.db[con.migration_collection].insert_one(
            for j in watching:
                if l.name == j.name:
                    continue
                for user in set(l.leaving.keys()) & set(j.joining.keys()):
                    if (max(l.leaving[user], j.joining[user]) -
                       min(l.leaving[user], j.joining[user])) < related_limit:
                        print(('user {0} left stream {1} and went to stream ' +
                              ' {2}').format(user, l.name, j.name))


def update_stream(streamname):
    global watching

    stream = list(filter(lambda s: s.name == streamname, watching))
    if len(stream) != 1:
        print(('{} is not in watching. Something wen\'' +
              'wrong').format(streamname))
        return
    stream = stream[0]

    with print_lock:
        print('Entering thread: ' + stream.name)
    users = get_users(stream.name)
    #  if the users list is empty something wen't wrong
    if len(users) == 0:
        return

    stream.update_watching(set(users))
    stream.remove_stale_joiners(join_ttl)
    stream.remove_stale_leavers(leave_ttl)

    for i, s in enumerate(watching):
        if s.name == streamname:
            watching[i] = stream

    with print_lock:
        print('Exiting thread: ' + stream.name)
        print('\n\n{}\n\n'.format(threading.active_count()))


def get_users(channel):
    """
    takes in a channel name and gathers the list of users currently signed into
    twitch and watching that channel
    @return: returns an array of strings that are users watching this channel
    """
    global headers
    global irc_lock
    global api_lock

    with irc_lock:
        # print(threading.current_thread().name + ' getting users for: ' +
        #       channel)
        users = irc.get_channel_users(channel)
        # print((threading.current_thread().name + ' length of users gotten ' +
        #       'from IRC is {0}').format(len(users)))

    #  print random string to make reading terminal easier
    # print(''.join(random.choice(string.ascii_uppercase + string.digits) for _
    #       in range(5)))

    if len(users) < irc_min_users:
        with api_lock:
            # print(('IRC user count is less than {0}, now double checking ' +
            #       'with API call').format(irc_min_users))
            users = api.get_users(channel)

    return users


def get_monitored_streams():
    #  get list of channels that are being monitored
    return con.db[con.monitoring_collection].find(
            {
                'list_category': 'main_list',
            },
            {
                '_id': False,
                'streams': True
            },
           )


if __name__ == '__main__':
    main()

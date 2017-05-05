import threading
from lib.irc_connect import IRCConnection
from lib.api_connect import APIConnection
from lib.db_connect import NoSQLConnection
from stream import Stream
import time
# import pdb

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
irc_min_users = 20

# the limit for joining and leaving entries to be considered related
related_limit = 300

# the limit for two migrations to be stored in the database
# if there is another migration where the user leaves and joins the same
# streams within leaving_overlap_limit seconds of the current migration
# then the current migration won't be added
leaving_overlap_limit = 300
joining_overlap_limit = 300

# This controls the delay in thread start in order to more evenly distribute
# work
thread_start_delay = 0.2

# the number of seconds between updating monitored_streams
check_monitored_streams_interval = 10

print_lock = threading.RLock()
irc_lock = threading.RLock()
api_lock = threading.RLock()

monitored_streams = set()


def main():
    global monitored_streams

    # get initial monitored_streams
    monitored_streams = get_monitored_streams()
    last_monitored_streams_update = time.time()

    # for each stream in monitored_streams create a thread for it
    threads = []
    for stream in monitored_streams:
        t = threading.Thread(target=update_stream, name=stream.name + '-thread',
                             args=(stream,))
        threads.append(t)
        t.start()
        time.sleep(thread_start_delay)

    # continuously update monitored_streams and for any new streams create a new thread
    # also join any dead threads, indicating that that stream is no longer in
    # monitored_streams
    while True:
        # if check_monitored_streams_interval seconds have passed since the last check
        if time.time() - last_monitored_streams_update >= check_monitored_streams_interval:
            current_monitored_streams = get_monitored_streams()
            # streams that are in the current_monitored_streams but not in
            # the old monitored_streams need to have a thread created
            new_monitored_streams = current_monitored_streams.difference(monitored_streams)
            # update the old monitored_streams to match the current_monitored_streams
            monitored_streams = current_monitored_streams

            for stream in new_monitored_streams:
                t = threading.Thread(target=update_stream, name=stream.name + '-thread',
                                     args=(stream,))
                threads.append(t)
                t.start()
                time.sleep(thread_start_delay)

        # also check for any threads that aren't running any more and join them back into the main thread
        for thread in threads:
            if not thread.isAlive():
                thread.join()
                print('thread {} has stopped being monitored and has been removed'.format(thread.name))
                threads.remove(thread)


def update_stream(stream):
    global monitored_streams

    with print_lock:
        print('Entering thread: ' + stream.name)

    while True:
        #  check that this stream is in the list of streams to watch
        if stream not in monitored_streams:
            # this thread is no longer in monitored_streams and should kill itself
            return

        users = get_users(stream.name)

        #  TODO possibly include remove_stale_joiners and remove_stale_leavers in
        #  the update_watching function
        stream.update_watching(set(users))
        stream.remove_stale_joiners(join_ttl)
        stream.remove_stale_leavers(leave_ttl)

        #  replace this stream in monitored_streams
        monitored_streams.add(stream)

        record_viewcount(stream)

        for j in monitored_streams:
            if j.name == stream.name:
                continue
            # only record migrations from this stream to all other streams
            record_migrations(stream, j)

        # with print_lock:
        #     print('Exiting thread: ' + stream.name)
        #     print('\n\n{}\n\n'.format(threading.active_count()))


def record_viewcount(stream):
    """
    records the current viewcount of stream in the database
    """
    #  record current viewcount
    con.viewercount_collection.insert_one(
        {
            'streamname': stream.name,
            'user_count': len(stream.watching),
            'time': time.time()
        })


def record_migrations(from_stream, to_stream):
    """
    finds any users who have moved left from_stream and joined to_stream and records these
    in the database
    @return: doesn't return anything, instead stores it in the database
    """
    #  for every user that has left l and joined j
    for user in set(from_stream.leaving.keys()) & set(to_stream.joining.keys()):
        # the difference between leaving and joining time
        migration_time = abs(from_stream.leaving[user] - to_stream.joining[user])
        if migration_time < related_limit:

            # print(('user {0} left stream {1} and went to stream ' +
            #        '{2}').format(user, from_stream.name, to_stream.name))
            add_migration_to_db(from_stream.name, to_stream.name, user,
                                from_stream.leaving[user],
                                to_stream.joining[user])


def add_migration_to_db(from_stream, to_stream, user, leave_time, join_time):
    """
    adds the migration from from_stream to to_stream of user user, that left at leave_time and joined
    at join_time to the database. First checks to see if there is another migration within a certain time
    """
    # if user == 'mroseman_bot':
    #     pdb.set_trace()

    # If there is no document for migrations from from_stream to to_stream
    # create it
    con.migrations_collection.update_one(
        {
            'from_stream': from_stream,
            'to_stream': to_stream
        },
        {
            '$set': {
                'from_stream': from_stream,
                'to_stream': to_stream
            },
            '$setOnInsert': {
                'migrations': []
            }
        },
        upsert=True)

    # Insert this new migration of user from from_stream to to_stream into the
    # list of migrations.
    # only insert if there is no document in migrations with either leave_time or
    # join_time within leaving_overlap_limit or joining_overlap_limit seconds respectively
    # of this leave_time and join_time
    con.migrations_collection.update_one(
        {
            'from_stream': from_stream,
            'to_stream': to_stream,
            # only add this migration when there is no previous document
            # that is within joining_overlap_limit or leaving_overlap_limit
            '$nor': [
                {
                    'migrations': {
                        # select documents where leave_time is within leaving_overlap_limit
                        # or join_time is within joining_overlap_limit of this new docs
                        # leave and join time
                        '$elemMatch': {
                            '$or': [
                                {
                                    'username': user,
                                    'leave_time': {
                                        '$gte': leave_time - leaving_overlap_limit
                                    }
                                },
                                {
                                    'username': user,
                                    'join_time': {
                                        '$gte': join_time - joining_overlap_limit
                                    }
                                }
                            ]
                        }
                    }
                }
            ]
        },
        {
            '$push': {
                'migrations': {
                    'username': user,
                    'leave_time': leave_time,
                    'join_time': join_time
                }
            }
        },
        upsert=False)


def get_users(channel):
    """
    takes in a channel name and gathers the list of users currently signed into
    twitch and watching that channel
    @return: returns a set of strings that are users watching this channel
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

    if len(users) == 0:
        # TODO throw exception or print warning
        pass

    return users


def get_monitored_streams():
    """
    get the list of channels that are being monitored
    @return: a set of Stream objects representing the
        streams that should be monitored
    """
    #  get list of channels that are being monitored
    streams = con.monitoring_collection.find(
        {
            'list_category': 'main_list',
        },
        {
            '_id': False,
            'streams': True
        },
    )
    return set([Stream(stream['streamname']) for stream in streams[0]['streams']])


if __name__ == '__main__':
    main()

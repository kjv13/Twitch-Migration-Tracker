import time
import threading
from irc_connect import IRCConnection
from api_connect import APIConnection
from db_connect import NoSQLConnection

print('connecting to noSQL database')
con = NoSQLConnection()

print('connecting to IRC server')
irc = IRCConnection()

print('creating API connection')
api = APIConnection()

# Constants
#  the time to live for the joining user elements in database
join_ttl = 300
#  this is the number of seconds before leave elements are
#  removed
leave_ttl = 300

#  if the number of users from IRC is less then 100 then
# the result is double checked with an API call
irc_min_users = 100

print_lock = threading.Lock()
irc_lock = threading.Lock()
api_lock = threading.Lock()


class update_thread (threading.Thread):

    def __init__(self, streamname, name=None):
        super().__init__(name=name)
        self.stream = streamname

    def run(self):
        users = get_users(self.stream)
        #  if the users list is empty something wen't wrong
        if len(users) == 0:
            return

        users_joining = []
        users_leaving = []
        #  really these are an array of json objects
        joining_json = []
        leaving_json = []

        # figure out any new users and any users that have left
        old_users = con.db[con.watching_collection].find(
                {
                    # find all documents with 'streamname' = stream
                    'streamname': self.stream
                },
                {
                    # project only the watching field
                    '_id': False,
                    'watching': True
                }
        )
        #  if this stream is in the database get joining and leaving users
        if old_users.count() == 1:
            old_users = set(old_users[0]['watching'])
            new_users = set(users)
            #  users in new_users that aren't in old_users
            users_joining = new_users - old_users
            #  users in old_users that are no longer in new_users
            users_leaving = old_users - new_users

        # with print_lock:
        #     print('\njoining users in {0}: {1}'.format(self.stream,
        #           users_joining))
        #     print('leaving users in {0}: {1}\n'.format(self.stream,
        #           users_leaving))

        #  create the new joining and leave json to add to the database
        for user in users_joining:
            joining_json.append(
                {
                    'username': user,
                    'last_updated': time.time()
                }
            )
        for user in users_leaving:
            leaving_json.append(
                {
                    'username': user,
                    'last_updated': time.time()
                }
            )

        #  add these users to this streams document
        #  TODO make sure the joining and leaving json appends to the list and
        #  that any elements that have expiredd are removed

        # with print_lock:
        #     print('updating watching users in the database')

        con.db[con.watching_collection].update_one(
            {'streamname': self.stream},
            {
                '$set': {
                    'watching': users,
                },
                '$push': {
                    'joining': {
                        '$each': joining_json,
                    },
                    'leaving': {
                        '$each': leaving_json
                    }
                },
                '$setOnInsert': {
                    'streamname': self.stream,
                }
            }
        )
        #  removing any stale joining or leaving users
        con.db[con.watching_collection].update_one(
            {'streamname': self.stream},
            {
                '$pull': {
                    'joining': {
                        'last_updated': {
                            '$lte': time.time() - join_ttl
                        }
                    },
                    'leaving': {
                        'last_updated': {
                            '$lte': time.time() - leave_ttl
                        }
                    }
                }
            }
        )


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
        print(threading.current_thread().name + ' getting users for: ' +
              channel)
        users = irc.get_channel_users(channel)
        print((threading.current_thread().name + ' length of users gotten ' +
              'from IRC is {0}').format(len(users)))

    #  print random string to make reading terminal easier
    # print(''.join(random.choice(string.ascii_uppercase + string.digits) for _
    #       in range(5)))

    if len(users) < irc_min_users:
        with api_lock:
            print(('IRC user count is less than {0}, now double checking ' +
                  'with API call').format(irc_min_users))
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


while True:
    print("""

    -------------------------------------------------------
    streams updated now starting from beginning
    -------------------------------------------------------

    """)

    streams = get_monitored_streams()
    streams = [stream['streamname'] for stream in streams[0]['streams']]

    con.db[con.watching_collection].delete_many(
            {
                'streamname':
                {
                    #  delete any documents where the streamname is not present
                    #  in streams
                    '$nin': streams
                }
            }
    )

    for stream in streams:
        t = update_thread(stream, stream+'-thread')
        t.daemon = True
        t.start()
    while threading.active_count() > 0:
        continue

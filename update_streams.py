import requests
import urllib
import time
from db_connect import NoSQLConnection


# Constants
# time in seconds between top stream updates
update_interval = 60
# the number of top games to get
game_limit = 20
# the number of streams to get for each game (as long as
# they are over the viewer limit
stream_limit = 20
# the minimum number of viewers a stream must have to be
# monitored
viewer_limit = 200
# the amount of seconds a stream is kept in the monitoring table before being
# removed
stream_ttl = 600

print('connecting to database')
nosql_con = NoSQLConnection()

streams = []
headers = {'Client-ID': 'sdu5b9af6eoqgkxdkb0qrkd9fgcp6ch'}


def get_top_streams(game_name):
    """
    find the top channels for a specific game on twitch
    @param game_name: the name of the game to search
    @return: no return, simply adds streams to global streams list
    """
    global streams

    print(('requesting top {0} streams for game {1} from' +
          'API').format(stream_limit, game_name))
    game_name = game_name
    payload = urllib.parse.urlencode({
                'game': game_name,
                'limit': str(stream_limit),
                'stream_type': 'live',
                'language': 'en'
              })
    r = requests.get('https://api.twitch.tv/kraken/streams',
                     params=payload, headers=headers).json()
    for stream in r['streams']:
        #  if the stream has enough viewers to be monitored
        if stream['viewers'] >= viewer_limit:
            streams.append(stream['channel']['name'])


while True:
    print('requesting top {0} games from API'.format(game_limit))
    payload = {'limit': str(game_limit)}
    r = requests.get('https://api.twitch.tv/kraken/games/top',
                     params=payload, headers=headers).json()

    for game in r['top']:
        game_name = game['game']['name']
        get_top_streams(game_name)

    print('adding these streams to database to be monitored...')
    json_streams = []
    for stream in streams:
        print('\t{0}'.format(stream))
        json_streams.append(
            {
                'streamname': stream,
                'last_updated': time.time(),
            }
        )

    nosql_con.db[nosql_con.monitoring_collection].update_one(
        {'list_category': 'main_list'},
        {
            '$push': {
                'streams': {
                    '$each': json_streams,
                }
            }
        },
        True
    )
    nosql_con.db[nosql_con.monitoring_collection].update_one(
        {'list_category': 'main_list'},
        {
            '$pull': {
                'streams': {
                    'last_updated': {
                        '$lte': time.time() - stream_ttl
                    }
                }
            }
        }
    )

    time.sleep(update_interval)

from lib.db_connect import NoSQLConnection
from lib.api_connect import APIConnection
import time


# Constants
# time in seconds between top stream updates
update_interval = 60
# the number of top games to get
game_limit = 15
# the number of streams to get for each game (as long as
# they are over the viewer limit
stream_limit = 15
# the minimum number of viewers a stream must have to be
# monitored
viewer_limit = 200
# the amount of seconds a stream is kept in the monitoring table before being
# removed
stream_ttl = 600

print('connecting to database')
nosql_con = NoSQLConnection()

print('creating API connection')
api = APIConnection()


def main():
    while True:
        # clear streams for the new current games
        streams = []
        print('requesting top {0} games from API'.format(game_limit))
        for game in api.get_top_games(game_limit):
            streams += api.get_top_streams(game, stream_limit, viewer_limit)

        print('WARNING: ABOUT TO DELETE ALL MONITORED STREAMS AND STARTING' +
              'AFRESH. THIS WAS IMPLEMENTED FOR TESTING PURPOSES.')
        # NOTE This is for testing purposes, delete for production
        nosql_con.monitoring_collection.update(
            {'list_category': 'main_list'},
            {
                '$set': {
                    'streams': []
                }
            }
        )

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

        # add these streams to the main list of streams to monitor
        nosql_con.monitoring_collection.update_one(
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

        # remove any stale streams that havn't been added for at
        # least stream_ttl seconds
        nosql_con.monitoring_collection.update_one(
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

        print(('\nmonitored_streams updated, now waiting {}' +
               ' seconds\n').format(update_interval))
        time.sleep(update_interval)


if __name__ == '__main__':
    main()

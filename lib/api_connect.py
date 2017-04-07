import sys
import requests
import time
import configparser
import os

# the mimnimum number of viewers a stream must have to be
# monitored
viewer_limit = 200
# the number of seconds that will be waited if a 503 response is
# gotten
timeout = 2
# the number of times to retry the request before giving up
num_tries = 3


class APIBadRequest(Exception):
    pass


class APIConnection:
    """
    Used to connect to the twitch api server and send http requests to it
    """

    current_dir = os.path.dirname(__file__)
    config_rel_path = '../config/api.cfg'
    config_abs_path = os.path.join(current_dir, config_rel_path)

    section_name = 'Connection Authentication'

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(self.config_abs_path)

        try:
            self._client_id = config[self.section_name]['client_id']
            self.headers = {'Client-ID': self._client_id}
        except Exception as e:
            print('one of the options in the config file has no value\n{0}:' +
                  '{1}').format(e.errno, e.strerror)
            sys.exit()

        self.log = open('watching_log.txt', 'w')

        self.last_api_call = time.time() - 1

    def _send_request(self, request, params=None):
        """
        sends an api request with params and headers
        keeps sending if there is a 503 result (times out for 'timeout'
        seconds
        """
        time.sleep(max(1 - (time.time() - self.last_api_call), 0))

        for i in range(0, num_tries):
            result = requests.get(request, params=params,
                                  headers=self.headers)
            if self._valid_result(result):
                return result.json()
            else:
                print(('\n{0} request failed, waiting {1} seconds to try' +
                      'again\n').format(i, timeout))
            time.sleep(timeout)
        raise APIBadRequest('after {0} tries the api continues to send bad' +
                            'results')

    def _valid_result(self, result):
        """
        checks the status code of the result
        """
        if result.status_code == 200:
            return True
        else:
            self.log.write(str(time.time()) +
                           ': ' + str(result.status_code) +
                           '\n')
            print(result.status_code)
            return False

    def get_top_games(self, limit):
        """
        gets the top 'limit' games from twitch
        @return: a list of top game names
        """
        games = []
        payload = {'limit': str(limit)}
        result = self._send_request('https://api.twitch.tv/kraken/games/top',
                                    payload)
        for game in result['top']:
            games.append(game['game']['name'])

        return games

    def get_top_streams(self, game, limit):
        """
        returns the 'limit' top streams for a game 'game'
        """
        streams = []
        payload = {
            'game': game,
            'limit': str(limit),
            'stream_type': 'live',
            'language': 'en'
        }
        result = self._send_request('https://api.twitch.tv/kraken/streams',
                                    payload)

        for stream in result['streams']:
            #  if the stream has enough viewers to be monitored
            if stream['viewers'] >= viewer_limit:
                streams.append(stream['channel']['name'])

        return streams

    def get_users(self, channel):
        """
        gets all the viewers of a specific channel
        """
        users = []
        url = 'https://tmi.twitch.tv/group/user/{0}/chatters'.format(channel)
        result = self._send_request(url)

        usercount = result['chatter_count']
        print('usercount = {0}'.format(usercount))
        if (usercount > 0):
            result = result['chatters']

            if result['moderators']:
                users = result['moderators']
            if result['staff']:
                users = users + result['staff']
            if result['admins']:
                users = users + result['admins']
            if result['global_mods']:
                users = users + result['global_mods']
            if result['viewers']:
                users = users + result['viewers']

        return users

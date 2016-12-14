from datetime import datetime
from datetime import timedelta


# Currently I have streamer containing sets of users
# I don't think this currently makes the most sense
# because of a couple of reasons
# It is difficult to remove elements from the set that meat a certain condition
# It is difficult to update the ttl of elements when there is a duplicate
# I beleive I need to create my own hash table to store this info

class Streamer:
    """
    Representation of a streamers watching users, leaving users, and joining
    users
    """

    def __init__(self, streamer_name, watchers=set(), joiners=set(),
                 leavers=set()):
        self.streamer_name = streamer_name
        self.watchers = watchers
        self.joiners = joiners
        self.leavers = leavers

    def update_users(self, current_users):
        """
        Updates the set of users watching this streamer, while adding any new
        users to the joining set, and any leaving users to the leaving set
        @param current_users: a set of the currently watching users (all of
            them not just the new ones
        @return: None
        """
        old_users = self.watchers
        users_joining = current_users - old_users
        users_leaving = old_users - current_users

        self.watchers = current_users
        self.joiners |= users_joining
        self.leavers |= users_leaving

    def remove_stale_users(self, ttl):
        """
        Updates the sets of joining and leaving users, removing those who have
        lived beyond the ttl
        @param ttl: an integer representing the ttl in seconds
        @return: None
        """


class User:
    """Representation of a user watching a streamer"""

    def __init__(self, username, last_updated=datetime.now()):
        self.username = username
        self.last_updated = last_updated

    def __eq__(self, other):
        return self.username == other.username

    def __hash__(self):
        return hash(self.username)

    def is_stale(self, ttl):
        """
        Calculates if the time since this user was last updated is greater than
        the time to live.
        @param ttl: an integer representing the amount of time this user should
            live in seconds
        @return: True if this user is stale and should be removed, or false if
            it is not stale yet
        """
        d = timedelta(seconds=ttl)
        return datetime.now() > (self.last_updated + d)

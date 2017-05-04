import time


class Stream():

    def __init__(self, streamname):
        self.name = streamname
        self.watching = set()
        self.joining = {}
        self.leaving = {}

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.name)

    def update_watching(self, current_watchers):
        """
        updates the watching key in watching. This completely replaces
        watching
        @param current_watchers: the new set of watchers this stream object with
        record
        @return: None
        """
        old_watchers = self.watching
        # users in old_watchers but not in current_watchers would have left
        leaving = old_watchers.difference(current_watchers)
        # users in current_watchers but weren't in old_watchers would have joined
        joining = current_watchers.difference(old_watchers)
        self.watching = current_watchers
        self.update_joining(joining)
        self.update_leaving(leaving)

    def update_joining(self, new_joiners):
        """
        updates the joining key in joining. This joins the new_joiners and
        current joiners like a set. old_joiners | new_joiners, where duplicates
        favor new_joiners and the last_updated value is taken from there
        @param new_joiners: a set of new joining usernames
        @return: None
        """
        # new_joining_dict is of the form
        # {
        #  'username': 'last_updated',
        #  'username': 'last_updated',
        #  ...
        # }
        new_joining_dict = dict.fromkeys(new_joiners, time.time())
        # this adds any new username keys from new_joining_dict into the
        # joining dictionary. Any duplicates are updated with the new
        # last_updated time
        self.joining = dict(self.joining, **new_joining_dict)

    def update_leaving(self, new_leavers):
        """
        updates the leaving key in leaving. This joins the new_leavers and
        current leavers like a set. old_leavers | new_leavers, where duplicates
        favor new_leavers and the last_updated value is taken from there
        @param new_leavers: a set of new leaving usernames
        @return: None
        """
        # new_leaving_dict is of the form
        # {
        #  'username': 'last_updated',
        #  'username': 'last_updated',
        #  ...
        # }
        new_leaving_dict = dict.fromkeys(new_leavers, time.time())
        # this adds any new username keys from new_leaving_dict into the
        # leaving dictionary. Any duplicates are updated with the new
        # last_updated time
        self.leaving = dict(self.leaving, **new_leaving_dict)

    def remove_stale_joiners(self, ttl):
        """
        removes any joiners from the joining dict whose last_updated is before
        current time - ttl
        @param: ttl the number of seconds a joiner should remain in the
        database. Any ones older than this are removed
        @return: None
        """
        self.joining = {joiner: join_time for joiner, join_time in
                        self.joining.items() if time.time() - join_time < ttl}

    def remove_stale_leavers(self, ttl):
        """
        removes any leavers from the leaving dict whose last_updated is before
        current time - ttl
        @param: ttl the number of seconds a leaver should remain in the
        database. Any ones older than this are removed
        @return: None
        """
        self.leaving = {leaver: leave_time for leaver, leave_time in
                        self.leaving.items() if time.time() - leave_time < ttl}

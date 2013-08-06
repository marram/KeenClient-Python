from keen.api import KeenApi
__author__ = 'dkador'


class BasePersistenceStrategy(object):
    """
    A persistence strategy is responsible for persisting a given event
    somewhere (i.e. directly to Keen, a local cache, a Redis queue, etc.)
    """

    def persist(self, event):
        """Persists the given event somewhere.

        :param event: the event to persist
        """
        raise NotImplementedError()


class DirectPersistenceStrategy(BasePersistenceStrategy):
    """
    A persistence strategy that saves directly to Keen and bypasses any local
    cache.
    """

    def __init__(self, project_id, write_key, read_key):
        """ Initializer for DirectPersistenceStrategy.

        :param api: the Keen Api object used to communicate with the Keen API
        """
        self.project_id = project_id
        self.write_key = write_key
        self.read_key = read_key
        self.make_api()
        super(DirectPersistenceStrategy, self).__init__()

    def make_api(self):
        self.api = KeenApi(self.project_id, write_key=self.write_key, read_key=self.read_key)

    def persist(self, event):
        """ Posts the given event directly to the Keen API.

        :param event: an Event to persist
        """
        self.api.post_event(event)

class AsyncAPIStrategy(DirectPersistenceStrategy):
    def make_api(self):
        self.api = KeenApi(self.project_id, write_key=self.write_key, read_key=self.read_key, async=True)

class RedisPersistenceStrategy(BasePersistenceStrategy):
    """
    A persistence strategy that persists events to Redis for later processing.

    Not yet implemented.
    """
    pass


class FilePersistenceStrategy(BasePersistenceStrategy):
    """
    A persistence strategy that persists events to the local file system for
    later processing.

    Not yet implemented.
    """
    pass

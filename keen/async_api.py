from google.appengine.api import urlfetch
from keen import exceptions
from keen.api import KeenApi
try:
    import ujson as json
except:
    from django.utils import simplejson as json
__author__ = 'dkador'


class AsyncKeenApi(KeenApi):
    """
    Responsible for communicating with the Keen API. Used by multiple
    persistence strategies or async processing.
    """

    def post_event(self, event):
        """
        Posts a single event to the Keen IO API. The write key must be set first.

        :param event: an Event to upload
        """
        if not self.write_key:
            raise Exception("The Keen IO API requires a write key to send events. "
                            "Please set a 'write_key' when initializing the "
                            "KeenApi object.")

        url = "{0}/{1}/projects/{2}/events/{3}".format(self.base_url, self.api_version,
                                                       self.project_id,
                                                       event.collection_name)
        headers = {"Content-Type": "application/json", "Authorization": self.write_key}
        payload = event.to_json()
        rpc = urlfetch.create_rpc()
        urlfetch.make_fetch_call(rpc, url=url,
                                payload=payload,
                                method=urlfetch.POST,
                                headers=headers)

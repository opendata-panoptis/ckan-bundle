import hashlib
import logging

from ckan.common import request
from ckan.types import Response

from ckanext.tracking.model import TrackingRaw


logger = logging.getLogger(__name__)


def track_request(response: Response) -> Response:
    path = request.environ.get('PATH_INFO')
    method = request.environ.get('REQUEST_METHOD')
    if path == '/_tracking' and method == 'POST':
        # Μαζεύουμε ό,τι χρειαζόμαστε όσο υπάρχει ακόμη request context.
        # Το callback εκτελείται αφού κλείσει το response iterable, οπότε τότε
        # το request context δεν θα είναι πλέον διαθέσιμο.
        url = request.form.get("url")
        tracking_type = request.form.get("type")
        if not url or not tracking_type:
            return response

        # we want a unique anonomized key for each user so that we do
        # not count multiple clicks from the same user.
        key = ''.join([
            request.environ['HTTP_USER_AGENT'],
            request.environ['REMOTE_ADDR'],
            request.environ.get('HTTP_ACCEPT_LANGUAGE', ''),
            request.environ.get('HTTP_ACCEPT_ENCODING', ''),
        ])
        # raises a type error on python<3.9
        h = hashlib.new('md5', usedforsecurity=False)
        h.update(key.encode())
        user_key = h.hexdigest()

        def _store_tracking() -> None:
            try:
                logger.debug("Tracking %s for %s", tracking_type, url)
                TrackingRaw.create(
                    user_key=user_key,
                    url=url,
                    tracking_type=tracking_type,
                )
            except Exception:
                logger.exception("Error tracking request")

        # Αναβάλουμε το DB write μέχρι να έχει σταλεί η απόκριση.
        if hasattr(response, "call_on_close"):
            response.call_on_close(_store_tracking)
        else:
            _store_tracking()

    return response

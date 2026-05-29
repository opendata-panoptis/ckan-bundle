# -*- coding: utf-8 -*-
"""
Βοηθητικές συναρτήσεις για versioned cache λεξιλογίων.
"""
import threading
import time

_VOCAB_CACHE_VERSION_KEY = 'ckanext:vocabulary_admin:cache_version'
_VOCAB_CACHE_FALLBACK_TTL_SECONDS = 30

_fallback_lock = threading.Lock()
_fallback_version = 1
_fallback_expires_at = 0.0


def _decode_int(value, default=1):
    if value is None:
        return default
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'ignore')
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_fallback_version():
    global _fallback_version, _fallback_expires_at
    now = time.time()
    with _fallback_lock:
        if now >= _fallback_expires_at:
            _fallback_version += 1
            _fallback_expires_at = now + _VOCAB_CACHE_FALLBACK_TTL_SECONDS
        return _fallback_version


def _bump_fallback_version():
    global _fallback_version, _fallback_expires_at
    with _fallback_lock:
        _fallback_version += 1
        _fallback_expires_at = time.time() + _VOCAB_CACHE_FALLBACK_TTL_SECONDS
        return _fallback_version


def get_vocabulary_cache_version():
    """
    Επιστρέφει την τρέχουσα shared έκδοση του cache.
    Αν δεν υπάρχει Redis, χρησιμοποιεί τοπικό fallback version.
    """
    try:
        from ckan.lib.redis import connect_to_redis  # type: ignore
        redis_conn = connect_to_redis()
        current = redis_conn.get(_VOCAB_CACHE_VERSION_KEY)
        if current is None:
            redis_conn.set(_VOCAB_CACHE_VERSION_KEY, 1, nx=True)
            current = redis_conn.get(_VOCAB_CACHE_VERSION_KEY)
        return _decode_int(current, default=1)
    except Exception:
        return _get_fallback_version()


def invalidate_vocabulary_cache():
    """
    Αυξάνει το shared cache version μετά από μεταβολή (write).
    """
    try:
        from ckan.lib.redis import connect_to_redis  # type: ignore
        redis_conn = connect_to_redis()
        return _decode_int(redis_conn.incr(_VOCAB_CACHE_VERSION_KEY), default=1)
    except Exception:
        return _bump_fallback_version()

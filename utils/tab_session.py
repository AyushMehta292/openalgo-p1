"""Flask session that keeps the broker out of the shared cookie.

One OpenAlgo sign-in is a cookie every tab sends. The broker is not. A second
tab can be on another broker at the same time, so ``broker``, ``FEED_TOKEN``
and ``USER_ID`` live on the request (``flask.g``) and are filled from the
``X-OpenAlgo-Broker`` header or from the URL. They are never written into the
cookie.
"""

from __future__ import annotations

from flask.sessions import SecureCookieSession, SecureCookieSessionInterface

TAB_KEYS = frozenset({"broker", "FEED_TOKEN", "USER_ID"})


def _tab_state() -> dict:
    from flask import g

    state = getattr(g, "_tab_state", None)
    if state is None:
        state = {}
        g._tab_state = state
    return state


class TabAwareSession(SecureCookieSession):
    """Cookie session plus per-request broker fields that are not serialized."""

    def __getitem__(self, key):
        if key in TAB_KEYS:
            tab = _tab_state()
            if key not in tab:
                raise KeyError(key)
            return tab[key]
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if key in TAB_KEYS:
            _tab_state()[key] = value
            return
        super().__setitem__(key, value)

    def __delitem__(self, key):
        if key in TAB_KEYS:
            _tab_state().pop(key, None)
            return
        super().__delitem__(key)

    def __contains__(self, key):
        if key in TAB_KEYS:
            return key in _tab_state()
        return super().__contains__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key, *args):
        if key in TAB_KEYS:
            tab = _tab_state()
            if key not in tab:
                if args:
                    return args[0]
                raise KeyError(key)
            return tab.pop(key)
        return super().pop(key, *args)

    def clear(self) -> None:
        _tab_state().clear()
        super().clear()


class TabSessionInterface(SecureCookieSessionInterface):
    session_class = TabAwareSession

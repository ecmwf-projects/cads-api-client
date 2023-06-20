
import functools
from typing import Dict, Any, Optional, List, Type, TypeVar

import attrs
import requests
from settings import CADS_API_URL, CADS_API_KEY

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")


def cads_raise_for_status(response: requests.Response) -> None:
    if response.status_code > 499:
        response.raise_for_status()
    if response.status_code > 399:
        error_json = None
        try:
            error_json = response.json()
        except Exception:
            pass
        if error_json is not None:
            raise RuntimeError(f"{response.status_code} Client Error: {error_json}")
        else:
            response.raise_for_status()


def get_links(response, rel: Optional[str] = None) -> List[Dict[str, str]]:
    if rel is None:  # edge case (don't loop)
        return []
    return [link for link in response.json().get("links", []) if link.get("rel") == rel]


def get_link_href(response, **kwargs: str) -> str:
    links = get_links(response, **kwargs)
    if len(links) != 1:
        raise RuntimeError(f"link not found or not unique {kwargs}")
    return links[0]["href"]


def from_rel_href(response: requests.Response, rel: str, session, headers) -> requests.Response:
    rels = get_links(response, rel=rel)
    assert len(rels) <= 1
    if len(rels) == 1:
        out = session.get(
            "get", url=rels[0]["href"], headers=headers, session=session
        )
    else:
        out = None
    return out


class ResponseIterator:
    def __init__(self, url, session, headers):

        self.session = session
        self.headers = headers
        self.url = url
        self.end = False
        self.page = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.end:
            raise StopIteration
        self.page += 1
        cur_resp = self.session.get(self.url, headers=self.headers)
        next_resp = from_rel_href(response=cur_resp, rel="next", session=self.session, headers=self.headers)
        if not next_resp:
            self.end = True
        self.url = next_resp

        return cur_resp


class ConnectionObject:
    def __init__(self, base_url=CADS_API_URL, api_key=CADS_API_KEY, session=requests.Session()):
        self.base_url = base_url
        self.session = session
        self.api_key = api_key

    @property
    def headers(self) -> Dict[str, str]:
        if self.api_key is None:
            raise ValueError("A valid API key is needed to access this resource")
        return {"PRIVATE-TOKEN": self.api_key}

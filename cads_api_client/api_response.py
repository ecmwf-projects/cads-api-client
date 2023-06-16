from __future__ import annotations

import functools
from typing import Dict, Any, Optional, List, Type, TypeVar

import attrs
import requests

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


@attrs.define(slots=False)
class ApiResponse:
    """
    HTTP response from CADS API with utils.
    """
    response: requests.Response
    headers: Dict[str, Any] = {}
    session: requests.Session = (requests.api,)  # type: ignore

    # TODO as __init__?
    @classmethod
    def from_request(
        cls: Type[T_ApiResponse],
        *args: Any,
        raise_for_status: bool = True,
        session: requests.Session = requests.api,  # type: ignore
        **kwargs: Any,
    ) -> T_ApiResponse:
        response = session.request(*args, **kwargs)
        if raise_for_status:
            cads_raise_for_status(response)
        self = cls(response, headers=kwargs.get("headers", {}), session=session)
        return self

    @functools.cached_property      # TODO as cached method (for consistency with requests)
    def json(self) -> Dict[str, Any]:
        return self.response.json()  # type: ignore

    def get_links(self, rel: Optional[str] = None) -> List[Dict[str, str]]:
        links = []
        for link in self.json.get("links", []):
            if rel is not None and link.get("rel") == rel:
                links.append(link)
        return links
        # if rel is None:  # edge case (don't loop)
        #     return []
        # return [link for link in self.json.get("links", []) if link.get("rel") == rel]

    def get_link_href(self, **kwargs: str) -> str:
        links = self.get_links(**kwargs)
        if len(links) != 1:
            raise RuntimeError(f"link not found or not unique {kwargs}")
        return links[0]["href"]

    def from_rel_href(self, rel: str) -> Optional[ApiResponse]:
        rels = self.get_links(rel=rel)
        assert len(rels) <= 1
        if len(rels) == 1:
            out = self.from_request(
                "get", url=rels[0]["href"], headers=self.headers, session=self.session
            )
        else:
            out = None
        return out


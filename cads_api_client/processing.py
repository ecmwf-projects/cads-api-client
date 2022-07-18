from typing import Any, Dict, List

from owslib import ogcapi


class Processing(ogcapi.API):  # type: ignore
    def processes(self) -> Dict[str, Any]:
        path = "processes"
        processes = self._request(path)
        assert isinstance(processes, dict)
        return processes

    def process_ids(self) -> List[str]:
        processes = self.processes()
        ids = [proc["id"] for proc in processes["processes"]]
        return ids

    def process(self, process_id: str) -> Dict[str, Any]:
        path = f"processes/{process_id}"
        process = self._request(path)
        assert isinstance(process, dict)
        return process


def get_process_id_from_links(links: List[Dict[str, str]]) -> str:
    for link in links:
        if link.get("rel") == "retrieve-process":
            href = link.get("href", "")
            api_url, _, process_id = href.rpartition("/processes/")
            if process_id == "":
                raise RuntimeError(f"Can not parse link href {href}")
            return process_id
    raise RuntimeError('No link with rel="retrieve-process"')

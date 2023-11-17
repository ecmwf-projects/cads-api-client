# cads-api-client

CADS API Python client

The `ApiClient` needs the `url` to the API root and a valid API `key` to access protected resources.
You can also set the `CADS_API_URL` and `CADS_API_KEY` environment variables.

It is possible (but not recommended) to use the API key of the anonymous user
`00112233-4455-6677-c899-aabbccddeeff`. This is used in anonymous tests and
it is designed to be the least performant option to access the system.

Draft Python API:

```python
>>> import os
>>> cads_api_key = os.getenv("CADS_API_KEY", "00112233-4455-6677-c899-aabbccddeeff")

>>> import cads_api_client
>>> client = cads_api_client.ApiClient(cads_api_key)
>>> collection = client.collection("reanalysis-era5-pressure-levels")
>>> collection.end_datetime()
datetime.datetime(2022, 7, 20, 23, 0)
>>> remote = client.retrieve(
...     collection_id="reanalysis-era5-pressure-levels",
...     product_type="reanalysis",
...     variable="temperature",
...     year="2022",
...     month="01",
...     day="01",
...     level="1000",
...     time="00:00",
...     target="tmp1-era5.grib",
... )  # blocks
>>> remote = collection.submit(
...     variable="temperature",
...     product_type="reanalysis",
...     year="2021",
...     month="01",
...     day="02",
...     time="00:00",
...     level="1000",
... )  # doesn't block
>>> remote.request_uid
'...'
>>> remote.status
'...'
>>> remote.download("tmp2-era5.grib")  # blocks
'tmp2-era5.grib'

```

Advanced usage:

```python
>>> headers = {"PRIVATE-TOKEN": cads_api_key}
>>> cads_api_root_url = client.url
>>> processing = cads_api_client.Processing(f"{cads_api_root_url}/retrieve", headers=headers)
>>> process = processing.process("reanalysis-era5-pressure-levels")
>>> status_info = process.execute(
...     inputs={
...         "variable": "temperature",
...         "product_type": "reanalysis",
...         "year": "2022",
...         "month": "01",
...         "day": "03",
...         "time": "00:00",
...         "level": "1000",
...     }
... )  # doesn't block
>>> remote = status_info.make_remote()
>>> remote_url = remote.url
>>> remote.request_uid
'...'
>>> remote.status  # the request is in the CADS cache
'...'
>>> del remote
>>> remote_replica = cads_api_client.Remote(remote_url)
>>> remote_unknown = processing.job("ffffffff-4455-6677-8899-aabbccddeeff").make_remote()
Traceback (most recent call last):
...
RuntimeError: 404 Client Error: ...

```

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.10:

```
conda create -n DEVELOP -c conda-forge python=3.10
conda activate DEVELOP
```

Before pushing to GitHub, run the following commands:

1. Update conda environment: `make conda-env-update`
1. Install this package: `pip install -e .`
1. Sync with the latest [template](https://github.com/ecmwf-projects/cookiecutter-conda-package) (optional): `make template-update`
1. Run quality assurance checks: `make qa`
1. Run tests: `make unit-tests`
1. Run the static type checker: `make type-check`
1. Build the documentation (see [Sphinx tutorial](https://www.sphinx-doc.org/en/master/tutorial/)): `make docs-build`

## License

```
Copyright 2022, European Union.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

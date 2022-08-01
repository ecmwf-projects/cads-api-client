# cads-api-client

CADS API Python client

Draft Python API:

```python
>>> import cads_api_client
>>> catalogue = cads_api_client.Catalogue("http://localhost:8080/api/catalogue")
>>> collection = catalogue.collection("reanalysis-era5-single-levels")
>>> collection.end_datetime()
<datetime64("2022-07-20T23:00:00")>
>>> remote = collection.retrieve(variable='2m_temperature', date='2022-07-01')  # doesn't block
>>> remote.request_uid
"123e4567-e89b-12d3-a456-426614174000"
>>> remote.status
"queued"
>>> remote.to_grib("data.grib")  # blocks until the file can be downloaded
>>> remote.to_dataset()  # uses locally cached data
<Dataset>
...

```

Adanced usage:

```python
>>> processing = cads_api_client.Processing("http://localhost:8080/api/processing")
>>> remote = processing.retrieve(
...     collection_id="reanalysis-era5-single-levels", variable='2m_temperature', date='2022-07-01'
... )  # doesn't block
>>> remote.request_uid
"123e4567-e89b-12d3-a456-426614174000"
>>> remote.status  # the request is in the CADS cache
"success"
>>> del remote
>>> remote_repica = processing.make_remote("123e4567-e89b-12d3-a456-426614174000")
>>> remote_repica.to_dataset()  # uses locally cached data
<Dataset>
...

>>> remote_unkown = processing.make_remote("ffffffff-e89b-12d3-a456-426614174000")
ValueError: request_uid is unkown
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

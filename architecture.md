
dal client posso interrogare sia catalogue api sia processing (retrieve) api. sul catalogue ho tutti i dataset 
presentati secondo la specifica STAC (credo), mentre sulla retrieve api ho tutti i processi che posso lanciare 
presentati secondo la specifica OGC Processes. la prima (catalogue) è più che altro di esplorazione, mentre l'altra 
(retrieve) serve per comunicare con il compute layer.


APIResponse: classe per le risposte. contiene utilities per gestire le risposte dell'API di CDS
--> in realtà la classe contiene anche la sessione

Abbiamo degli API client che impacchettano in una classe le chiamate da fare all'API che ci interessano.

- collection
- process
- job

```json
{'type': 'Collection',
   'id': 'reanalysis-era5-pressure-levels',
   'stac_version': '1.0.0',
   'title': 'ERA5 hourly data on pressure levels from 1940 to present',
   'description': 'ERA5 is the fifth generation ECMWF reanalysis for the global climate and weather for the past 8 ...',
   'keywords': ['Variable domain: Atmosphere (upper air)',
    'Provider: Copernicus C3S',
    'Spatial coverage: Global',
    'Temporal coverage: Past',
    'Variable domain: Atmosphere (surface)',
    'Product type: Reanalysis'],
   'license': 'proprietary',
   'extent': {'spatial': {'bbox': [[0.0, -89.0, 89.0, 360.0]]},
    'temporal': {'interval': [['1959-01-01T00:00:00Z',
       '2023-05-09T00:00:00Z']]}},
   'links': [{'rel': 'self',
     'type': 'application/json',
     'href': 'https://cds2-dev.copernicus-climate.eu/api/catalogue/v1/collections/reanalysis-era5-pressure-levels'},
    {'rel': 'parent',
     'type': 'application/json',
     'href': 'https://cds2-dev.copernicus-climate.eu/api/catalogue/v1/'},
    {'rel': 'root',
     'type': 'application/json',
     'href': 'https://cds2-dev.copernicus-climate.eu/api/catalogue/v1/'},
    {'rel': 'license',
     'href': 'https://s3.cds.ecmwf.int/swift/v1/AUTH_3e237111c3a144df8e0e0980577062b4/cds2-dev-catalogue/licences/licence-to-use-copernicus-products/licence-to-use-copernicus-products_b4b9451f54cffa16ecef5c912c9cebd6979925a956e3fa677976e0cf198c2c18.pdf',
     'title': 'Licence to use Copernicus Products'}],
   'assets': {'thumbnail': {'href': 'https://s3.cds.ecmwf.int/swift/v1/AUTH_3e237111c3a144df8e0e0980577062b4/cds2-dev-catalogue/resources/reanalysis-era5-pressure-levels/overview_3a600b4d477bd7226c5fabb17ba38f8c34c21be6363603c196fca1c58d87dfa2.png',
     'type': 'image/jpg',
     'roles': ['thumbnail']}},
   'published': '2018-06-14T00:00:00Z',
   'updated': '2023-05-15T00:00:00Z',
   'sci:doi': '10.24381/cds.bd0915c6'}
```


```json
{
  'title': 'CAMS global reanalysis (EAC4)',
  'description': 'EAC4 (ECMWF Atmospheric Composition Reanalysis 4) is the fourth generation ECMWF global reanalysis ...',
  'id': 'cams-global-reanalysis-eac4',
  'version': '1.0.0',
  'jobControlOptions': ['async-execute'],
  'outputTransmission': ['reference'],
  'links': [{
    'href': 'https://cds2-dev.copernicus-climate.eu/api/retrieve/v1/processes/cams-global-reanalysis-eac4', 
    'rel': 'process',
    'type': 'application/json',
    'title': 'process description'
  }]
}
```


catalogue/datasets
catalogue/collections/{collection_id}
catalogue/vocabularies/licences

retrieve/processes
retrieve/processes/{process_id}
retrieve/processes/{process_id}/execute

retrieve/jobs
retrieve/jobs/{job_id}
retrieve/jobs/{job_id}/results
retrieve/jobs/{job_id}

profiles/account
profiles/account/licences/{licence_id}
profiles/account/licences

We want to have consistency between the client and the underlying API.

For consistency with the underlying API, split Processing class into ProcessesAPIClient and JobsAPIClient.
ProcessList and Process go into ProcessesAPIClient. Remote becomes JobsAPIClient. StatusInfo and Results become Job, 
so that we have Job and JobList to be managed by JobsAPIClient.


catalogue/collections/{collection_id} returns info about the API endpoint to call to retrieve data. 
we don't want the responses to become clients, but we want to be able to give a response as input to another API client.

CatalogueAPIClient             ProcessesAPIClient          JobsAPIClient
collection                     -> process                  -> job
    .get_process("retrieve")       .submit(request)           .download()

We don't want the CatalogueAPIClient to be able to launch a job and get its status. We want to orchestrate these 
behaviours only in the general APIClient.

---

We would like to provide a fluent interface so that we could launch a job in the form:

collection.retrieve_process().submit(request).status

To achieve that we should return APIClient objects.

client = cads_api_client.ApiClient(url=api_url, key="00112233-4455-6677-c899-aabbccddeeff")
collection = client.collection("reanalysis-era5-pressure-levels")
collection.retrieve_process().execute(inputs=request, accepted_licences=accepted_licences).make_remote().status

---

se i client diventano fluenti c'è ancora bisogno dell'api client generale? 
come si passano i token? --> si restituisce un client già autenticato


---


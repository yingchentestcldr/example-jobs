# Configuration

1. create a python virtual env

```bash
$ python -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

2. deactivate the current python virtual env

```bash
$ deactivate
```

# Setup

1. open inspect window of your browser and copy a POST call as cURL

```bash
curl 'https://p6rfnnxd.cde-9qs9lx7k.apps.shared-rke-dev-01.kcloud.cloudera.com/dex/api/v1/info' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0' -H 'Accept: application/json, text/plain, _/_' -H 'Accept-Language: en-US,en;q=0.5' -H 'Accept-Encoding: gzip, deflate, br, zstd' -H 'authorization: Bearer THIS_IS_FAKE_eyJqa3UiOiJodHRwczpcL1wvc2VydmljZS5jZGUtOXFzOWx4N2suYXBwcy5zaGFyZWQtcmtlLWRldi0wMS5rY2xvdWQuY2xvdWRlcmEuY29tXC9nYXRld2F5XC9jZHB0a25cL2tub3h0b2tlblwvYXBpXC92MVwvandrcy5qc29uIiwia2lkIjoiQ2E0MWdPQVdMc3lYV2toZWQ3OW83dGhEVzJ0ZWtjU2lWRVltdVpIWlI0VSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJjZHBfeGh1Iiwiamt1IjoiaHR0cHM6XC9cL3NlcnZpY2UuY2RlLTlxczlseDdrLmFwcHMuc2hhcmVkLXJrZS1kZXYtMDEua2Nsb3VkLmNsb3VkZXJhLmNvbVwvZ2F0ZXdheVwvY2RwdGtuXC9rbm94dG9rZW5cL2FwaVwvdjFcL2p3a3MuanNvbiIsImtpZCI6IkNhNDFnT0FXTHN5WFdraGVkN1NpVkVZbXVaSFpSNFUiLCJpc3MiOiJLTk9YU1NPIiwiZXhwIjoxNzIyMDM1NzYwLCJtYW5hZ2VkLnRva2VuIjoiZmFsc2UiLCJrbm94LmlkIjoiYzJkMzI4YmYtOTZmMS00MDAwLThmZGEtMjczYzYxOGQ5ZGJlIn0.WASh74HI5aaBlbawvxjwWVB9jx5d-4vZ0kcunJq7AGTbRjpNbiL5vpwAgM9tLFaA4yNn5FJIJmMKuF9OBpyWjFOzb2JNW-QlpTmGoaNbOmwOWkdgAs3EVt6jTKM7dX4u7dXst8M7F8f6ddpqQCB5tb00bI0n3G8PaAoZw9fDqMaCPldRT1emzqrVPkFvW0NbkM1dmN9AlhUTKaJEHFltu8b9BpU-RxSKfCcqVlYOCQOT6cvZFd8-4htoYAfJsdoRJxfOLQF1GD10le3TCD4fdGUF8Slgwhj_BBQwE1nY3ZKaIHXCH07akmpZTqA_CbaVQDX32QNYmz-4TfvLov1WAw' -H 'ngsw-bypass: ' -H 'Origin: https://console-xhu-200.apps.shared-rke-dev-01.kcloud.cloudera.com' -H 'Connection: keep-alive' -H 'Referer: https://console-xhu-200.apps.shared-rke-dev-01.kcloud.cloudera.com/' -H 'Sec-Fetch-Dest: empty' -H 'Sec-Fetch-Mode: cors' -H 'Sec-Fetch-Site: same-site' -H 'TE: trailers'
```

copy the content of header `authorization`

2. open API doc window and click the `lock` icon and paste the content of `authorization` header.
   ![API doc window](images/api-doc.png)

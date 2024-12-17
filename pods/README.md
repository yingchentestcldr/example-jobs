# run pyenv-builder pod

1. update the following field in the `pyenv-builder.yaml` file
   a. metadata.name
   b. metadata.namespace
   c. spec.containers[0].env[3].value (for DEX_PYTHON_DESCRIBE_RESOURCE_URL)
   d. spec.containers[0].image
   e. spec.priorityClassName
   f. spec.serviceAccount
   g. spec.serviceAccountName
   h. spec.volumes[0].persistentVolumeClaim.claimName

# run pip3 install

```bash
$ pip3 install requests-kerberos -i https://pypi.infra.cloudera.com/api/pypi/cloudera/simple --trusted-host pypi.infra.cloudera.com
```

full command run by the actual python env builder:

```bash
$ bash -c "source /app/dex/storage/resources/virtual-env-1/python-virtual-environment/bin/activate && python3 -m compileall -q -f /app/dex/storage/resources/virtual-env-1/python-virtual-environment && pip3 install -r /app/dex/storage/resources/virtual-env-1/requirements.txt -i https://pypi.infra.cloudera.com/api/pypi/cloudera/simple --trusted-host pypi.infra.cloudera.com"
```

```bash
$ bash -c "source /app/dex/storage/resources/virtual-env-1/python-virtual-environment/bin/activate && python3 -m compileall -q -f /app/dex/storage/resources/virtual-env-1/python-virtual-environment && pip3 install -r /app/dex/storage/resources/virtual-env-1/requirements.txt"
```

create a virtual env

```bash
$ virtualenv /app/dex/storage/resources/virtual-env-1/python-virtual-environment --python=python3
$ virtualenv /app/dex/storage/resources/virtual-env-1/python-virtual-environment --python=python3 --verbose
$ virtualenv /app/dex/storage/resources/virtual-env-1/python-virtual-environment --python=python3 --verbose --no-setuptools --no-pip --no-wheel
$ python -m ensurepip
```

```golang
Note This module does not access the internet. All of the components needed to bootstrap pip are included as internal parts of the package.
```

create a requirements.txt file

```bash
$ echo "requests-kerberos" > /app/dex/storage/resources/virtual-env-1/requirements.txt
pip        21.3.1
setuptools 59.6.0
```

spark-kinit-init

KRB5_CONFIG=/etc/krb5/krb5.conf kinit -kt /etc/krb5/krb5.keytab/u---00uihumisdpsp9ttp5d7-krb5-secret -p cdp_xhu@ROOT.COMOPS.SITE

# unplug Ozone from Data Services

[PARTNER-7785](https://jira.cloudera.com/browse/PARTNER-7785)

```bash
$ CDP_ACCESS_KEY_ID=a CDP_PRIVATE_KEY=b cdp --endpoint-url https://console-test-listenv.apps.shared-os-qe-05.kcloud.cloudera.com --no-verify-tls environments set-environment-setting --environment-name "test-listenv-env-14" --settings '{ "logType":"OZONE", "ozoneS3Key":"key", "ozoneS3Secret":"secret", "ozoneLogsBucket":"cloudera", "ozoneLogsPath":"/apath", "ozoneS3RestUrl":"s3url" }'
$ CDP_ACCESS_KEY_ID=fake-one CDP_PRIVATE_KEY=fake-one cdp --endpoint-url https://console-xhu-200.apps.shared-rke-dev-01.kcloud.cloudera.com environments get-environment-setting --environment-name xhu-200-env-1 --no-verify-tls --attrs logType
```

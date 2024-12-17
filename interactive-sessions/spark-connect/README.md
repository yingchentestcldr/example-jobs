## Helm install spark-connect chart

```bash
$ helm install sc-xzrvwf2x charts/apps/spark-connect --set image.repository=docker-sandbox.infra.cloudera.com/xhu/spark-connect,image.buildNumber=dex-12099-xhu-3 --set ingress.hostSubdomain=cde-77p6xr26.apps.shared-os-qe-01.kcloud.cloudera.com --set ingress.ingressClassName=cde-ingress-cluster-77p6xr26
```

```bash
$ podman run -it --entrypoint /bin/sh docker-sandbox.infra.cloudera.com/xhu/spark-connect:1.21.0-dex-12099-xhu-3
$ pyspark --remote "sc://spark-connect-server-i1:80"
$ export CDE_SC_HOST=sc-sgvcjr27.cde-262dk76v.apps.shared-rke-dev-01.kcloud.cloudera.com
$ export HELM_REL_NAME=sc-xzrvwf2x
$ export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/usr/local/lib/python3.8/site-packages/certifi/cacert.pem
$ export CDE_TOKEN=eyJraWQiOm51bGwsImFsZyI6IkVTMjU2In0.eyJpc3MiOiJBbHR1cyBJQU0iLCJhdWQiOiJBbHR1cyBJQU0iLCJqdGkiOiJSV001aWVzRnNHUl8xdXF6VkNVSm9RIiwiaWF0IjoxNzIzMDk5MzU5LCJleHAiOjE3MjMxNDI1NTksInN1YiI6ImNybjphbHR1czppYW06dXMtd2VzdC0xOmRiNTc5NGEyLWMwNzctNDhiYi05YmRkLTNjMWJmZWMzNTE1Yzp1c2VyOmRjZGMzNTZmLWNmNWEtNGZkMC05NGFiLWE3YWIwNzI3N2M1NyJ9.xh91QxOaDuyo3XW3o715G0qRLMhEGjdGD8Ahd938n3qROVqyQcJHiUhk_9lH4lFzmEKnhkdiX9n3vCx-BycO2Q
# this following one will NOT work, as it does not have auth token
$ pyspark --remote "sc://spark-connect-server.cde-dtp6t7gl.apps.shared-rke-dev-01.kcloud.cloudera.com:443/;use_ssl=true"
# this one works, and routes to the first spark-connect server
$ pyspark --remote "sc://$CDE_SC_HOST:443/;use_ssl=true;x-cde-session-name=$HELM_REL_NAME-spark-connect-i1;token=$CDE_TOKEN"
$ pyspark --remote "sc://$CDE_SC_HOST:443/;use_ssl=true;x-cde-session-name=test-session;token=$CDE_TOKEN"
# this one works, and routes to the second spark-connect server
$ pyspark --remote "sc://$CDE_SC_HOST:443/;use_ssl=true;x-cde-session-name=$HELM_REL_NAME-spark-connect-i2;token=$CDE_TOKEN"
# this one will NOT work, as the i3 spark-connect server does not exist
$ pyspark --remote "sc://spark-connect-server.cde-dtp6t7gl.apps.shared-rke-dev-01.kcloud.cloudera.com:443/;use_ssl=true;spark-suffix=i3;token=$CDE_TOKEN"
```

```python
>>> df = spark.read.json("../examples/src/main/resources/people.json")
>>> df.show()
```

### generate a pkey/CA cert

```bash
$ openssl genpkey -algorithm RSA -out cde-xhu-ca.key -aes256
$ openssl req -x509 -new -nodes -key cde-xhu-ca.key -sha256 -days 1825 -out cde-xhu-ca.pem
$ cat cde-xhu-ca.pem >> .venv/lib/python3.10/site-packages/certifi/cacert.pem
$ cat letsencrypt-stg-root-x1.pem >> .venv/lib/python3.10/site-packages/certifi/cacert.pem
```

### create a ca cert for ingress host

```bash
$ export CDE_SC_HOST=sc-sgvcjr27.cde-262dk76v.apps.shared-rke-dev-01.kcloud.cloudera.com
$ cp ../cde-tools-xhu/cde-xhu-ca.pem .
$ cp ../cde-tools-xhu/cde-xhu-ca.key .
$ openssl genrsa -out $CDE_SC_HOST.key 2048
$ openssl req -new -key $CDE_SC_HOST.key -out $CDE_SC_HOST.csr
$ vi $CDE_SC_HOST.ext
$ openssl x509 -req -in $CDE_SC_HOST.csr -CA cde-xhu-ca.pem -CAkey cde-xhu-ca.key \
-CAcreateserial -out $CDE_SC_HOST.crt -days 825 -sha256 -extfile $CDE_SC_HOST.ext
$ openssl x509 -in $CDE_SC_HOST.crt -text -noout
$ kubectl create secret tls sc-tls-secret --cert=$CDE_SC_HOST.crt --key=$CDE_SC_HOST.key
# ingress might not work, use route instead on OCP
$ oc create route edge grpc-test --service=grpc
# assuming cacert contains the root CA
$ grpcurl -cacert cacert -import-path . -proto service.proto grpc-test-xhu-tools.apps.shared-os-dev-02.kcloud.cloudera.com:443 Server.Foo
$ grpcurl -cacert cacert grpc-test-xhu-tools.apps.shared-os-dev-02.kcloud.cloudera.com:443 describe
```

Openshift support for http/2 and gRPC connections

```bash
$ oc -n openshift-ingress-operator annotate ingresscontrollers/default ingress.operator.openshift.io/default-enable-http2=true
```

```bash
$ grpcurl -plaintext localhost:15002 describe
$ grpcurl -insecure spark-connect-server.cde-8rpnqqpj.apps.test-rosa-01.kcloud.cloudera.com:443 describe
grpc.reflection.v1alpha.ServerReflection is a service:
service ServerReflection {
  rpc ServerReflectionInfo ( stream .grpc.reflection.v1alpha.ServerReflectionRequest ) returns ( stream .grpc.reflection.v1alpha.ServerReflectionResponse );
}
spark.connect.SparkConnectService is a service:
service SparkConnectService {
  rpc AddArtifacts ( stream .spark.connect.AddArtifactsRequest ) returns ( .spark.connect.AddArtifactsResponse );
  rpc AnalyzePlan ( .spark.connect.AnalyzePlanRequest ) returns ( .spark.connect.AnalyzePlanResponse );
  rpc Config ( .spark.connect.ConfigRequest ) returns ( .spark.connect.ConfigResponse );
  rpc ExecutePlan ( .spark.connect.ExecutePlanRequest ) returns ( stream .spark.connect.ExecutePlanResponse );
}
```

1. enable CDE ingress controller to support h2c
   a. modify nginx.tmpl to listen to http2 on port 81
   b. create cm for nginx.tmpl

   ```bash
   $ kubectl create cm nginx-template --from-file=nginx.tmpl=charts/apps/spark-connect/nginx.tmpl
   ```

   b. add port 81 to svc and make appProtocol: h2c
   c. create an ingress that uses port 81 to proxy from OCP router to CDE ingress controller
   d. modify deployment to expose port 81

2. create ingress with CDE ingress-class, set backend-protocol to GRPC.

modification to nginx ingress-controller deployment

```yaml
ports:
  - containerPort: 81
    name: h2c
    protocol: TCP
---
volumeMounts:
  - mountPath: /etc/nginx/template
    name: nginx-template-volume
    readOnly: true
---
volumes:
  - configMap:
      defaultMode: 420
      items:
        - key: nginx.tmpl
          path: nginx.tmpl
      name: nginx-template
    name: nginx-template-volume
```

modification to nginx ingress-controller service

```yaml
ports:
  - appProtocol: h2c
    name: h2c
    port: 81
    protocol: TCP
    targetPort: 81
```

new ingress for spark-connect that route traffic to CDE ingress-controller

OCP version

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  annotations:
    route.openshift.io/termination: edge
  name: spark-connect-server
  namespace: dex-base-8rpnqqpj
spec:
  rules:
    - host: spark-connect-server.cde-8rpnqqpj.apps.test-rosa-01.kcloud.cloudera.com
      http:
        paths:
          - backend:
              service:
                name: dex-base-nginx-8rpnqqpj-controller
                port:
                  number: 81
            path: /
            pathType: Prefix
  tls:
    - hosts:
        - sc-sgvcjr27.cde-262dk76v.apps.shared-rke-dev-01.kcloud.cloudera.com
      secretName: sc-tls-secret
```

### Generate Workload Auth Token using CDP Access Key

```bash
$ cdp --form-factor private iam generate-workload-auth-token \
   --workload-name DE \
   --environment-crn crn:altus:environments:us-west-1:bdc02dc7-b656-46c3-aa9c-255b8f5722e8:environment:xhu-140-env-1/190b283f-7178-41f7-9e98-201749ab65c4 \
   --profile xhu-140 \
   --auth-config ~/.cdp/credentials
```

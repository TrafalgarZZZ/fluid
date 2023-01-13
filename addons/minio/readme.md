# Minio

## Apply ThinRuntimeProfile for Minio storage

```shell
kubectl apply -f runtime-profile.yaml
```

## How to use

### Create Secret to store credentials for Minio
```
kubectl create secret generic minio-secret \
  --from-literal=minio-access-key=<access-key> \ 
  --from-literal=minio-access-secret=<access-secret>
```

### Create Dataset and ThinRuntime CRs

```yaml
$ cat <<EOF > dataset.yaml
apiVersion: data.fluid.io/v1alpha1
kind: Dataset
metadata:
  name: minio-demo
spec:
  mounts:
  - mountPoint: minio://my-first-bucket # Minio bucket name 
    name: oss
    options:
      minio-url: http://minio:9000 # Accessible minio url endpoint
    encryptOptions:
      - name: minio-access-key
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: minio-access-key
      - name: minio-access-secret
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: minio-access-secret
---
apiVersion: data.fluid.io/v1alpha1
kind: ThinRuntime
metadata:
  name: minio-demo
spec:
  profileName: minio-profile # Specify the `minio-profile` created before
EOF
```

### Run pod with Fluid PVC

```shell
$ cat <<EOF > app.yaml
apiVersion: v1
kind: Pod
metadata:
  name: nginx
spec:
  containers:
    - name: nginx
      image: nginx
      volumeMounts:
        - mountPath: /data
          name: minio-demo
  volumes:
    - name: minio-demo
      persistentVolumeClaim:
        claimName: minio-demo
EOF

$ kubectl apply -f app.yaml
```
After the application using the remote file system is deployed, the corresponding FUSE pod is also scheduled to the same node.

```shell
$ kubectl get pods
NAME                     READY   STATUS    RESTARTS   AGE
minio-69c555f4cf-n48mf   1/1     Running   0          2d1h
minio-demo-fuse-l66jv    1/1     Running   0          104s
nginx                    1/1     Running   0          104s
```
Checking with the following command, the minio file system is mounted to the /data directory of nginx pod. 
```shell
kubectl exec -it nginx -- findmnt /data
```

Expected result should be like:
```
TARGET SOURCE          FSTYPE OPTIONS
/data  my-first-bucket fuse   rw,nosuid,nodev,relatime,user_id=0,group_id=0,defa
```

## Versions

* 0.1

Add init support for Minio.
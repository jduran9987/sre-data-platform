# Cluster setup

How to stand up the Meridian data platform from scratch on a fresh machine.

Most of the platform is declarative (manifests in `kubernetes/`, Helm releases in
`helmfile.yaml`). A few steps configure the cluster *itself*, below the Kubernetes
API, so they can't be manifests — they're listed here and must be run by hand, in
order, **before** any `kubectl apply`.

## Prerequisites

Local tools:

- `minikube`, `kubectl`, `helm`
- `helmfile` — plus the `helm-diff` plugin (`helm plugin install https://github.com/databus23/helm-diff`), which `helmfile apply` uses to preview changes.

## Bootstrap order

### 1. Create the cluster

```bash
minikube start --profile sre-data --nodes 3 --cpus 4 --memory 16384 --disk-size 50g --driver docker
```

Sizing: 3 nodes × (4 vCPU / 16 GB) = 12 vCPU / 48 GB, leaving headroom on the host.
Adjust to your machine, and make sure Docker Desktop's own resource limits are raised
to at least what you request here.

Verify all three nodes reach `Ready`:

```bash
kubectl get nodes
```

### 2. Enable the CSI hostpath storage driver (required)

```bash
minikube addons enable volumesnapshots -p sre-data
minikube addons enable csi-hostpath-driver -p sre-data
```

**Why this is required, not optional.** Strimzi runs Kafka as a non-root user and
relies on the pod's `fsGroup` to make its data volume writable. minikube's *default*
`standard` StorageClass provisions `hostPath` volumes, and the kubelet does **not**
apply `fsGroup` ownership to hostPath volumes — so the broker pods fail to start with
`java.nio.file.AccessDeniedException: /var/lib/kafka/data/...`. The CSI hostpath driver
honors `fsGroup` like a real cloud CSI driver. Our Kafka manifest pins its storage to
the `csi-hostpath-sc` StorageClass this addon creates.

Confirm the StorageClass exists before continuing:

```bash
kubectl get storageclass   # expect: csi-hostpath-sc
```

### 3. Create the shared namespace

```bash
kubectl apply -f kubernetes/namespaces/data-platform.yaml
```

### 4. Install the Strimzi Kafka operator (helmfile)

```bash
helmfile -l name=strimzi-kafka-operator apply
```

Verify the operator pod is `Running` in `data-platform` and the `kafka`,
`kafkanodepools`, `kafkatopics` CRDs are registered.

> The `kube-prometheus-stack` release in `helmfile.yaml` (observability) is installed
> later in the project — not part of this initial Kafka bootstrap.

### 5. Deploy the Kafka cluster

```bash
kubectl apply -f kubernetes/kafka/kafka-cluster.yaml
```

This takes a couple of minutes to converge. Verify:

```bash
kubectl -n data-platform get kafka,kafkanodepool,pods,pvc
```

Healthy state: `kafka/meridian` shows `READY: True`; 3 controller + 3 broker pods
`Running 1/1`; all PVCs `Bound` on `csi-hostpath-sc`.

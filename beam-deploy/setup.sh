#### create minikube cluster
minikube start --cpus='max' --memory=20480 --addons=metrics-server --kubernetes-version=v1.25.3

#### kafka
## download and deploy strimzi oeprator
STRIMZI_VERSION="0.39.0"
DOWNLOAD_URL=https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI_VERSION/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
curl -L -o kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml ${DOWNLOAD_URL}

sed -i 's/namespace: .*/namespace: default/' kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
kubectl create -f kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml

kubectl create -f kafka/manifests/kafka-cluster.yaml
kubectl create -f kafka/manifests/kafka-ui.yaml

#### if kafka cluster gets pending due to waiting for a volume to be created ...
## https://minikube.sigs.k8s.io/docs/tutorials/volume_snapshots_and_csi/
## enable addons
minikube addons enable volumesnapshots
minikube addons enable csi-hostpath-driver
## use it as a default storage class for the dynamic volume claims
minikube addons disable storage-provisioner
minikube addons disable default-storageclass
kubectl patch storageclass csi-hostpath-sc -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

# kubectl get all -l app.kubernetes.io/instance=demo-cluster

## local test
kubectl port-forward svc/kafka-ui 8080
kubectl port-forward svc/demo-cluster-kafka-external-bootstrap 29092

BOOTSTRAP_SERVERS=localhost:29092 python kafka/client/producer.py

## deploy
# use docker daemon inside minikube cluster
eval $(minikube docker-env)
# Host added: /home/jaehyeon/.ssh/known_hosts ([127.0.0.1]:32772)

docker build -t=kafka-client:0.1.0 kafka/client/.

kubectl create -f kafka/manifests/kafka-client.yml

## delete resources
kubectl delete -f kafka/manifests/kafka-cluster.yaml
kubectl delete -f kafka/manifests/kafka-ui.yaml
kubectl delete -f kafka/manifests/kafka-client.yml
kubectl delete -f kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml

#### flink
## flink operator
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
# helm install -f beam/values.yml flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator

## beam word_count
kubectl create -f beam/word_count_cluster.yml
kubectl create -f beam/word_count_job.yml

kubectl port-forward svc/beam-word-len-rest 8081

kubectl delete -f beam/word_count_cluster.yml
kubectl delete -f beam/word_count_job.yml

## beam word_len
kubectl create -f beam/word_len_cluster.yml
kubectl create -f beam/word_len_job.yml

kubectl port-forward svc/word-len-cluster-rest 8081

kubectl delete -f beam/word_len_cluster.yml
kubectl delete -f beam/word_len_job.yml

## beam example
# kubectl get all -l app=beam-word-len
kubectl create -f beam/word_len.yml
kubectl logs -f deploy/beam-word-len

kubectl delete flinkdeployment/beam-word-len

kubectl create -f beam/word_len_expansion.yml
kubectl create -f beam/word_len_cluster.yml
kubectl create -f beam/word_len_job.yml
kubectl create -f beam/beam_wordcount.yml

kubectl describe job beam-word-len-job
kubectl describe job beam-word-len-job

kubectl port-forward svc/beam-word-len-cluster-rest 8081
kubectl describe flinkdeployment/beam-word-len-cluster
kubectl describe flinksessionjobs beam-word-len-job

kubectl describe job beam-word-len-job
kubectl describe job beam-wordcount

kubectl delete -f beam/word_len_expansion.yml
kubectl delete -f beam/word_len_cluster.yml
kubectl delete -f beam/word_len_job.yml
kubectl delete -f beam/beam_wordcount.yml

kubectl delete flinkdeployment/beam-word-len-cluster
kubectl delete flinksessionjobs beam-word-len-job

## flink example
# kubectl get all -l app=flink-word-len
kubectl create -f flink/word_len.yml
kubectl logs -f deploy/flink-word-len
kubectl port-forward svc/flink-word-len-rest 8081
kubectl delete flinkdeployment/flink-word-len

#### delete minikube
minikube delete

#### create minikube cluster
minikube start --cpus='max' --memory=10240 --addons=metrics-server --kubernetes-version=v1.25.3

#### kafka
## download and deploy strimzi oeprator
STRIMZI_VERSION="0.39.0"
DOWNLOAD_URL=https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI_VERSION/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
curl -L -o kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml ${DOWNLOAD_URL}

sed -i 's/namespace: .*/namespace: default/' kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
kubectl create -f kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml

kubectl create -f kafka/manifests/kafka-cluster.yaml
kubectl create -f kafka/manifests/kafka-ui.yaml

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
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml

helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator

kubectl create -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.8/examples/basic.yaml
kubectl logs -f deploy/basic-example
kubectl port-forward svc/basic-example-rest 8081
kubectl delete flinkdeployment/basic-example

kubectl create -f word_len.yml
kubectl logs -f deploy/beam-word-len
kubectl port-forward svc/beam-word-len-rest 8081
kubectl delete flinkdeployment/beam-word-len

#### delete minikube
minikube delete

## create minikube cluster
minikube start --cpus='max' --memory=10240 --addons=metrics-server --kubernetes-version=v1.25.3

## download and deploy strimzi oeprator
STRIMZI_VERSION="0.39.0"
DOWNLOAD_URL=https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI_VERSION/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
curl -L -o kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml ${DOWNLOAD_URL}

sed -i 's/namespace: .*/namespace: default/' kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
kubectl create -f kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml

kubectl create -f kafka/manifests/kafka-cluster.yaml
kubectl create -f kafka/manifests/kafka-ui.yaml

## local test
minikube service kafka-ui --url
minikube service demo-cluster-kafka-external-bootstrap --url

BOOTSTRAP_SERVERS=127.0.0.1:39101 python kafka/client/producer.py

## deploy
# use docker daemon inside minikube cluster
eval $(minikube docker-env)
# Host added: /home/jaehyeon/.ssh/known_hosts ([127.0.0.1]:32772)

docker build -t=kafka-client:0.1.0 kafka/client/.

kubectl create -f kafka/manifests/kafka-client.yml

## delete resources
kubectl delete -f kafka/manifests/kafka-cluster.yaml
kubectl delete -f kafka/manifests/kafka-client.yml
kubectl delete -f kafka/manifests/kafka-ui.yaml
kubectl delete -f kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml

## delete minikube
minikube delete

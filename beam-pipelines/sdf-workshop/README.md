
# Splittable DoFns in Python

This repository contains the code samples used for the workshop "Splittable 
DoFns in Python" of the Beam Summit 2022:
* https://2022.beamsummit.org/sessions/splittable-dofns-in-python/

There are two branches in this repo:
* `main`: template to follow the workshop; write your code here.
* `solution`: full solutions provided in this branch. Check only after you have tried to write your own code.

The slides used during the workshop are available [here](docs/slides.pdf).

# Dependencies

Check the `requirements.txt` file and install those dependencies before 
trying to run the examples in this repo.

You will need Python 3.7, 3.8 or 3.9. Other versions of Python will not work.

If you want to use Kafka as well as the synthetic pipelines, you will need 
to install minikube, or alternatively, provide a Kafka server of your own. 
More details to install minikube and Kafka are given below.

# Synthetic pipelines

There are two pipelines in this repo using synthetic data: one for a batch 
example, and another one for a streaming example.

## Batch pipeline

To launch the batch pipeline, simply run 

`python my_batch_pipeline.py`

The pipeline generates some pseudo-files, and reads the files by chunks 
using a splittable DoFn. The code of the `DoFn` is in 
`mydofns/synthetic_sdfn_batch.py`.

**You need to write your solution for that splittable DoFn in that file**.

## Streaming pipeline

To launch the batch pipeline, simply run 

`python my_streaming_synth_pipeline.py`

In the file `mydofns/synthetic_sdfn_streaming.py`, in line 62, you can set 
the number of partitions for this streaming synthetic connector. By default, it is `NUM_PARTITIONS = 4`.

**You need to write your solution for that splittable DoFn in that file**.

# Pipeline using Kafka

Before you can use the pipeline with Kafka, you will need a Kafka server. In 
the next section you have instructions to run Kafka locally with minikube.

## Install Kafka

If you want to test your code against an actual Kafka server, follow the 
next steps to install Kafka in a local minikube cluster.

* Install minikube: https://minikube.sigs.k8s.io/docs/start/
* Make sure that you have an alias 
  - `alias k=kubectl`
* Create a namespace for Kafka: 
  - `k create namespace kafka`
* Install Kafka operator
  - `k create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka`
* Install single ephemeral cluster:
  - `k apply -f manifests/kafka-cluster.yaml -n kafka`
* Find out the port where Kafka is listening, and take note of it:
  - `k get service my-cluster-kafka-external-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}' -n kafka`
* Find out the local IP where Kafka is lesting, and take note of it:
  - `k get node minikube -o=jsonpath='{range .status.addresses[*]}{.type}{"\t"}{.address}{"\n"}'`

For your Kafka clients configuration, the bootstrap server will be `IP:PORT`.

## Topic creation and population

To test your pipeline against Kafka, you will need to write some data to 
Kafka. For that, the first step is to create a topic.

There is a Python script that can help you with the creation of the topic 
and the population with data.

Find out your bootstrap server details, and create an environment variable:

`export BOOTSTRAP=192.168.64.3:31457`

(in this example the IP is `192.168.64.3` and the port is `31457`; your details 
will be  different, please  use the IP of your minikube cluster and the  
port of your Kafka service, see above for more details)

To create the topic, run

`./kafka_single_client.py --bootstrap $BOOTSTRAP --create`

And to populate with data

`./kafka_single_client.py --bootstrap $BOOTSTRAP`

If you want to check that the topic is working correctly, you can run a 
consumer and check if there is data:

`./kafka_single_client.py --consumer --bootstrap $BOOTSTRAP`

## Pipeline using Kafka

To run the pipeline, use this script. The number of partitions is by default 4. Make sure that if you change the
number of partitions in `kafka_single_client.py`, you change it/use the same value in the Kafka `DoFn` too

`python my_streaming_kafka_pipeline.py --bootstrap $BOOTSTRAP `

The code of the `DoFn` functions is located in 
`mydofns/kafka_sdfn_streaming.py`.

**You need to write your solution for that splittable DoFn in that file**.
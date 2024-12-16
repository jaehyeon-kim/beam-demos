## Apache Beam Related Demo Projects

### Apache Beam Local Development With Python

[Apache Beam](https://beam.apache.org/) and [Apache Flink](https://flink.apache.org/) are open-source frameworks for parallel, distributed data processing at scale. Flink has DataStream and Table/SQL APIs and the former has more capacity to develop sophisticated data streaming applications. The DataStream API of PyFlink, Flink's Python API, however, is not as complete as its Java counterpart, and it doesn't provide enough capability to extend when there are missing features in Python. Recently I had a chance to look through Apache Beam and found it supports more possibility to extend and/or customise its features.

In this series of posts, we discuss local development of Apache Beam pipelines using Python. In _Part 1_, a basic Beam pipeline is introduced, followed by demonstrating how to utilise Jupyter notebooks for interactive development. Several notebook examples are covered including [Beam SQL](https://beam.apache.org/documentation/dsls/sql/overview/) and [Beam DataFrames](https://beam.apache.org/documentation/dsls/dataframes/overview/). Batch pipelines will be developed in _Part 2_, and we use pipelines from [GCP Python DataFlow Quest](https://github.com/GoogleCloudPlatform/training-data-analyst/tree/master/quests/dataflow_python) while modifying them to access local resources only. Each batch pipeline has two versions with/without SQL. Beam doesn't have its own processing engine and Beam pipelines are executed on a runner such as Apache Flink, Apache Spark, or Google Cloud Dataflow instead. We will use the [Flink Runner](https://beam.apache.org/documentation/runners/flink/) for deploying streaming pipelines as it supports [a wide range of features](https://beam.apache.org/documentation/runners/capability-matrix/) especially in streaming context. In _Part 3_, we will discuss how to set up a local Flink cluster as well as a local Kafka cluster for data source and sink. A streaming pipeline with/without Beam SQL will be built in _Part 4_, and this series concludes with illustrating unit testing of existing pipelines in _Part 5_.

- [Part 1 Pipeline, Notebook, SQL and DataFrame](https://jaehyeon.me/blog/2024-03-28-beam-local-dev-1/)
- [Part 2 Batch Pipelines](https://jaehyeon.me/blog/2024-04-04-beam-local-dev-2)
- [Part 3 Flink Runner](https://jaehyeon.me/blog/2024-04-18-beam-local-dev-3)
- [Part 4 Streaming Pipelines](https://jaehyeon.me/blog/2024-05-02-beam-local-dev-4/)
- [Part 5 Testing Pipelines](https://jaehyeon.me/blog/2024-05-09-beam-local-dev-5/)

### Deploy Python Stream Processing App on Kubernetes

Flink Kubernetes Operator acts as a control plane to manage the complete deployment lifecycle of Apache Flink applications. With the operator, we can simplify deployment and management of Python stream processing applications. In this series, we discuss how to deploy a PyFlink application and Python Apache Beam pipeline on the Flink Runner on Kubernetes.

- [Part 1 PyFlink Application](https://jaehyeon.me/blog/2024-05-30-beam-deploy-1/)
- [Part 2 Beam Pipeline on Flink Runner](https://jaehyeon.me/blog/2024-06-06-beam-deploy-2/)

### Apache Beam by Examples

Implement tasks in [Building Big Data Pipelines with Apache Beam](https://www.packtpub.com/product/building-big-data-pipelines-with-apache-beam/9781800564930) using the Beam Python SDK.

- [Part 1 Calculate K Most Frequent Words and Max Word Length](https://jaehyeon.me/blog/2024-07-04-beam-examples-1)
- [Part 2 Calculate Average Word Length with/without Fixed Look back](https://jaehyeon.me/blog/2024-07-18-beam-examples-2)
- [Part 3 Build Sport Activity Tracker with/without SQL](https://jaehyeon.me/blog/2024-08-01-beam-examples-3)
- [Part 4 Call RPC Service for Data Augmentation](https://jaehyeon.me/blog/2024-08-15-beam-examples-4)
- [Part 5 Call RPC Service in Batch using Stateless DoFn](https://jaehyeon.me/blog/2024-09-18-beam-examples-5)
- [Part 6 Call RPC Service in Batch with Defined Batch Size using Stateful DoFn](https://jaehyeon.me/blog/2024-10-02-beam-examples-6)
- [Part 7 Separate Droppable Data into Side Output](https://jaehyeon.me/blog/2024-10-24-beam-examples-7)
- [Part 8 Enhance Sport Activity Tracker with Runner Motivation](https://jaehyeon.me/blog/2024-11-21-beam-examples-8)
- [Part 9 Develop Batch File Reader and PiSampler using Splittable DoFn](https://jaehyeon.me/blog/2024-12-05-beam-examples-9)
- [Part 10 Develop Streaming File Reader using Splittable DoFn](https://jaehyeon.me/blog/2024-12-19-beam-examples-10)

## Beam Pipelines by Examples

Implement tasks in [Building Big Data Pipelines with Apache Beam](https://www.packtpub.com/product/building-big-data-pipelines-with-apache-beam/9781800564930) using the Beam Python SDK.

### Tasks

#### Chapter 2

- ✔️ Task 1 – Calculating the K most frequent words in a stream of lines of text
  - Description: Given an input data stream of lines of text, calculate the K most frequent words within a fixed time window of T seconds.
  - [Pipeline](./chapter2/top_k_words.py)
  - [Test](./chapter2/top_k_words_test.py)
- ✔️ Task 2 – Calculating the maximal length of a word in a stream
  - Description: Given an input data stream of lines of text, calculate the longest word ever seen in this stream. Start with an empty word value; once a longer word is seen, immediately output the new longest word.
  - [Pipeline V1](./chapter2/max_word_length.py)
  - [Test](./chapter2/max_word_length_test.py)
  - [Pipeline V2](./chapter2/max_word_length_with_ts.py)
  - [Test](./chapter2/max_word_length_with_ts_test.py)
- ✔️ Task 3 – Calculating the average length of words in a stream
  - Description: Given an input data stream of lines of text, calculate the average length of words currently seen in this stream. Output the current average as frequently as possible, ideally after every word.
  - [Pipeline](./chapter2/average_word_length.py)
  - [Test](./chapter2/average_word_length_test.py)
- ✔️ Task 4 – Calculating the average length of words in a stream with fixed lookback
  - Description: Given an input data stream of lines of text, calculate the average length of the words seen in this stream during the last 10 seconds and output the result every 2 seconds.
  - [Pipeline](./chapter2/sliding_window_word_length.py)
  - [Test](./chapter2/sliding_window_word_length_test.py)
- ✔️ Task 5 – Calculating performance statistics for a sport activity tracking application
  - Description: Given an input data stream of quadruples (workoutId, gpsLatitude, gpsLongitude, and timestamp) calculate the current speed and the total tracked distance of the tracker. The data comes from a GPS tracker that sends data only when its user starts a sport activity. We can assume that workoutId is unique and contains a userId value in it.
  - [Pipeline](./chapter2/sport_tracker.py)
  - [Test](./chapter2/sport_tracker_test.py)

#### Chapter 3

- ✔️ Task 6 – Using an external service for data augmentation
  - Description: Given an input stream of lines of text (coming from Apache Kafka) and an RPC service that returns a category (an integer) for each input word, use the integers returned from the RPC service to convert the stream of lines of text to a stream of pairs comprising the words with their respective integers.
  - [Pipeline](./chapter3/rpc_pardo.py)
  - [Test](./chapter3/rpc_pardo_test.py)
- ✔️ Task 7 – Batching queries to an external RPC service
  - Description: Given an RPC service that supports the batching of requests for increasing throughput, use this service to augment the input data of a PCollection object. Be sure to preserve the timestamp of both the timestamp and window assigned to the input element.
  - [Pipeline](./chapter3/rpc_pardo_batch.py)
  - [Test](./chapter3/rpc_pardo_batch_test.py)
- ✔️ Task 8 – Batching queries to an external RPC service with defined batch sizes
  - Description: Use a given RPC service to augment data in an input stream using batched RPCs with batches of a size of about K elements. Also, resolve the batch after a time of (at most) T to avoid a (possibly) infinitely long wait for elements in small batches.
  - [Pipeline](./chapter3/rpc_pardo_stateful.py)
  - [Test](./chapter3/rpc_pardo_stateful_test.py)
- ✔️ Task 9 – Separating droppable data from the rest of the data processing
  - Description: Create a pipeline that will separate droppable data elements from the rest of the data. It will send droppable data into one output topic and the rest into another topic.
  - [Pipeline](./chapter3/droppable_data_filter.py)
  - [Test](./chapter3/droppable_data_filter_test.py)
- ❌ Task 10 – Separating droppable data from the rest of the data processing, part 2
  - Description: Create a pipeline that will separate droppable data elements from the rest of the data elements. It will send droppable data to one output topic and the rest to another topic. Make the separation work even in cases when the very first element in a particular window is droppable.
  - Pipeline
  - Test

#### Chapter 4

- ⚒️ Task 11 – Enhancing SportTracker by runner motivation using side inputs
  - Description: Calculate two per-user running averages over the stream of artificial GPS coordinates that we generated for Task 5. One computation will be the average pace over a longer (5-minute) interval, while the other will be over a shorter (1-minute) interval. Every minute, for each user, output information will be provided regarding whether the user's current 1-minute pace is higher or lower than the longer average if the short average differs by more than 10%.
  - [Pipeline]()
  - [Test]()
- ⚒️ Task 12 – enhancing SportTracker by runner motivation using CoGroupByKey
  - Description: Implement Task 11 but instead of using side inputs, use CoGroupByKey to avoid any possible memory pressure due to forcing side input to fit into memory for all keys (user-tracks).
  - [Pipeline]()
  - [Test]()
- ⚒️ Task 13 – Writing a reusable PTransform – StreamingInnerJoin
  - Description: Implement a PTransform that will be applied to a PCollectionTuple with exactly two TupleTags (leftHandTag and rightHandTag) and will produce a streaming join operation on top of them. The transform needs to take functions to extract join keys and both sides' primary keys. The output will contain the (inner) joined values and any retractions.
  - [Pipeline]()
  - [Test]()

#### Chapter 5

- ⚒️ Task 14 – Implementing SQLMaxWordLength
  - Description: Given a stream of text lines in Apache Kafka, create a stream consisting of the longest word seen in the stream from the beginning to the present. Use triggering to output the result as frequently as possible. Use Apache Beam SQL to implement the task whenever possible.
  - [Pipeline]()
  - [Test]()
- ⚒️ Task 15 – Implementing SchemaSportTracker
  - Description: Given a stream of GPS locations and timestamps for a workout of a specific user (a workout has an ID that is guaranteed to be unique among all users), compute the performance metrics for each workout. These metrics should contain the total duration and distance elapsed from the start of the workout to the present.
  - [Pipeline]()
  - [Test]()
- ⚒️ Task 16 – Implementing SQLSportTrackerMotivation
  - Description: Given a GPS location stream per workout (the same as in the previous task), create another stream that would contain information if the runner increased or decreased pace in the past minute by more than 10% compared to the average pace over the last 5 minutes. Again, use SQL DSL as much as possible.
  - [Pipeline]()
  - [Test]()

#### Chapter 6

- ✔️ Task 17 – Implementing MaxWordLength in the Python SDK ⬅️ Task 2
- ✔️ Task 18 – Implementing SportTracker in the Python SDK ⬅️ Task 5
- ✔️ Task 19 – Implementing RPCParDo in the Python SDK ⬅️ Task 8
- ✔️ Task 20 – Implementing SportTrackerMotivation in the Python SDK ⬅️ Task 12

#### Chapter 7

- ⚒️ Task 21 – Implementing our own splittable DoFn – a streaming file source
  - Description: We want to create a streaming-like source from a directory on a filesystem that will work by watching a specified directory for new files. Once a new file appears, it will grab it and output its content split into individual (text) lines for downstream processing. The source will compute a watermark as a maximal timestamp for all of the files in the specified directory. For simplicity, ignore recursive sub-directories and treat all files as immutable.
  - [Pipeline]()
  - [Test]()
- ⚒️ Task 22 – A non-I/O application of splittable DoFn – PiSampler
  - Description: Create a Monte Carlo method for estimating the value of Pi. Use splittable DoFn to support distributed computation, specifying the (ideal) target parallelism and the number of samples drawn in each parallel worker
  - [Pipeline]()
  - [Test]()

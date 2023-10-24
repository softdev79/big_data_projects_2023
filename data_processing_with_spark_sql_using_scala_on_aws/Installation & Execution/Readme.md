# DATA PROCESSING WITH SPARK-SQL USING SCALA IN AWS

# HDFS vs Spark
The HDFS is a disk-based storage system(hard disk of data nodes), and using it frequently while processing data to store intermediate results is time-consuming. 

# Disk based systems - Types of Delays 
- Seek time : The time required to move the read/write head from its current position to a new position (or track) within a disk
- Rotational delay : The time taken to go from one side of the track to other side of the track
- Transfer time : The time taken to read/write a complete sector or a data chunk in  file.

# DIS-ADVANTAGES of HADOOP
- 1. Not suitable when you need results instantaneously
- 2. Processing data in batch mode takes longer
				
## IN_MEMORY PROCESSING
- IN-MEMORY processing is used when the data is stored in an in-memory data store or an in-memory data structure.
- ADVANTAGES of  IN_LINE PROCESSING :
- 1. used in real time processing od data in e-commerce industries as its response time is shorter.
- 2. since data is stored in memory and random access is possible, relavant data can be fetched directly without scanning the entire dataset.
- 3. Used in iterative applications where data is processed repeatedly as the intermediate outputgenerated is also stored in memory, saving you from time-consuming disk I/O operations. 

# Apache Spark
- SPARK is a fast IN_MEMORY data processing engine enables batch processing tasks quickly. 
- Also provisions streaming,machine learning , SQL workloads which requires fast iterative access to the datasets.

## Advantages of Spark

- Speed
- Generality

# Spark  Architecture
- Like Hadoop, Apache Spark also follows a master/slave architecture. 
- The daemon corresponding to the master runs on the master node, and similarly, the daemon corresponding to the slave runs on the slave node. 
- A cluster manager is an external service responsible for acquiring resources and allocating them to Spark jobs. 

## Spark Master : 
- The driver is responsible for creating a SparkContext. However big or small the Spark job, a SparkContext is mandatory to run it.
- The SparkContext generates a directed acyclic graph of different transformations performed on the RDD.
- The driver program splits the graph into multiple stages which are then assigned to the worker nodes via the cluster manager, to process data.

## Spark Slave

- Slaves are the nodes on which executors run.
- Executors are processes that run smaller tasks that are assigned by the driver.
- They store computed results either in memory, cache or on hard disks.

## Cluster Manager

- The cluster manager is responsible for providing the resources required to run tasks on the cluster.
- The driver program negotiates with the cluster manager to get the required resources, such as the number of executors, the CPU, the memory, etc.

# Iterative Processing (MapReduce vs Spark)
- An iterative task is one that uses a chain of jobs to determine the final output. In this set-up, mostly, the child job consumes the output of the parent job.
- In MapReduce, the output of the first job is stored on a disk. As this output is the input for the second job, the second job reads this data from the disk.
- In Spark,the intermediate output is stored in the RAM instead of on the disk.

# Components of the Spark Ecosystem
- Spark Core : interface between Spark components and the components of a big data processing architecture such as Yarn, the HDFS, etc.
- Spark SQL  : Spark SQL allows users to run SQL queries on top of Spark.Uses data frames and datasets that could impose a schema on RDDs, which are schema less.
- Spark Streaming : The Spark Streaming module processes streaming data in real time.The Spark Streaming module receives input data streams and divides them into mini-batches. 
- Spark MLlib : MLlib is a machine learning library that is used to run machine learning algorithms on big data sets that are distributed across a cluster of machines.
- Spark GraphX : It is used to understand how data is stored graphically as well as to understand cluster configurations.used to view data as a graph and combine graphs with RDDs.

# Scala Basics , Spark  - Transformations and Actions
- Understanding Array's,List,Tuples,Higher order functions
- Lambda Expression : The syntax of a lambda expression is - (Parameters) -> {Body}
- Accessing loops
- Creating RDD's and accessing the tuples in RDD using foreach

## Transformations : MAP,FLATMAP,FILTER
## Actions - Collect,COunt,First,take,show,saveas

## RDD's - Resilient Distributed Datasets
- RDD's are collections of data items that are broken into partitions and are stored in memory on the worker nodes of a cluster.
- Basic RDDs : These treat all the data items as a single value.
- Paired RDDs :  These treat all the data items as key-value pairs.
- Operations performed on RDD's are divided into two categories : Transformations , Actions

# DAG - Directed Acyclic Graph
- RDD Lineage using DAG
- Whenever an RDD is created, the new RDD point to its parent RDD
- Dependencies will be logged in Graph pattern
- Lazy Evaluation : This means the memory is not allocated when a rdd is defined , it will be assigned only when it is used.

## Finding Top 10 movies using RDD refer enclosed script (RDD-Top10Movies.scala)
## Finding Top 10 movies using SPARK SQL refer enclosed script (SparkSQL-Top10Movies.scala)
## TOP 3 Users based on the numer of review given refer enclose script (SparkSQL-Top3 Reviewers.scala)

## To execute Sample scala program from spark shell (Hello.scala)
- :load Hello.scala
- Hello.main(Array(" "))

- :load SparkSQL-Top3 Reviewers.scala

## Accessing HIVE tables (students.hql)
- In Spark-Shell to read HIVE table 
- create a Dataframe with the Query defnition
- perform action on created dataframe (example) df.show


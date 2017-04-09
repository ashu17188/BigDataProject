package org.ashu.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

object BasicQueryCassandra {
    def main(args: Array[String]) {
      val sparkMaster = "spark.master"
      val cassandraHost = "localhost"
      val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", cassandraHost)
      val sc = new SparkContext("local", "BasicQueryCassandra", conf)
      // entire table as an RDD
      // assumes your table test was created as CREATE TABLE test.kv(key text PRIMARY KEY, value int);
      val data = sc.cassandraTable("tutorialpoints" , "kv")
      // print some basic stats
      println("stats "+data.map(row => row.getInt("value")).stats())
      val rdd = sc.parallelize(List(("moremagic", 1)))
      rdd.saveToCassandra("tutorialpoints" , "kv", SomeColumns("key", "value"))
      // save from a case class
      val otherRdd = sc.parallelize(List(KeyValue("magic", 0)))
      otherRdd.saveToCassandra("tutorialpoints", "kv")
    }
}

case class KeyValue(key: String, value: Integer)


/*USE below command to create keyspace and table 
 
CREATE KEYSPACE tutorialpoints WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

 
CREATE TABLE tutorialpoints.kv (
    key text PRIMARY KEY,
    value int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

OUTPUT:-
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
17/04/09 15:03:19 INFO SparkContext: Running Spark version 2.1.0
17/04/09 15:03:25 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
17/04/09 15:03:28 WARN Utils: Your hostname, ashutosh-Inspiron-3551 resolves to a loopback address: 127.0.1.1; using 192.168.43.194 instead (on interface wlan0)
17/04/09 15:03:28 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
17/04/09 15:03:29 INFO SecurityManager: Changing view acls to: hduser
17/04/09 15:03:29 INFO SecurityManager: Changing modify acls to: hduser
17/04/09 15:03:29 INFO SecurityManager: Changing view acls groups to: 
17/04/09 15:03:29 INFO SecurityManager: Changing modify acls groups to: 
17/04/09 15:03:29 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hduser); groups with view permissions: Set(); users  with modify permissions: Set(hduser); groups with modify permissions: Set()
17/04/09 15:03:36 INFO Utils: Successfully started service 'sparkDriver' on port 49088.
17/04/09 15:03:37 INFO SparkEnv: Registering MapOutputTracker
17/04/09 15:03:37 INFO SparkEnv: Registering BlockManagerMaster
17/04/09 15:03:37 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
17/04/09 15:03:37 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
17/04/09 15:03:38 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-b7e26b83-2c4e-4f6e-8463-722be6d8dd28
17/04/09 15:03:39 INFO MemoryStore: MemoryStore started with capacity 332.1 MB
17/04/09 15:03:40 INFO SparkEnv: Registering OutputCommitCoordinator
17/04/09 15:03:44 INFO Utils: Successfully started service 'SparkUI' on port 4040.
17/04/09 15:03:44 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.43.194:4040
17/04/09 15:03:46 INFO Executor: Starting executor ID driver on host localhost
17/04/09 15:03:47 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 47103.
17/04/09 15:03:47 INFO NettyBlockTransferService: Server created on 192.168.43.194:47103
17/04/09 15:03:47 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
17/04/09 15:03:47 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.43.194, 47103, None)
17/04/09 15:03:47 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.43.194:47103 with 332.1 MB RAM, BlockManagerId(driver, 192.168.43.194, 47103, None)
17/04/09 15:03:47 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.43.194, 47103, None)
17/04/09 15:03:47 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.43.194, 47103, None)
17/04/09 15:03:58 INFO ClockFactory: Using native clock to generate timestamps.
17/04/09 15:03:58 INFO NettyUtil: Found Netty's native epoll transport in the classpath, using it
17/04/09 15:04:01 INFO Cluster: New Cassandra host localhost/127.0.0.1:9042 added
17/04/09 15:04:01 INFO CassandraConnector: Connected to Cassandra cluster: Test Cluster
17/04/09 15:04:03 INFO SparkContext: Starting job: stats at BasicQueryCassandra.scala:18
17/04/09 15:04:03 INFO DAGScheduler: Got job 0 (stats at BasicQueryCassandra.scala:18) with 4 output partitions
17/04/09 15:04:03 INFO DAGScheduler: Final stage: ResultStage 0 (stats at BasicQueryCassandra.scala:18)
17/04/09 15:04:03 INFO DAGScheduler: Parents of final stage: List()
17/04/09 15:04:03 INFO DAGScheduler: Missing parents: List()
17/04/09 15:04:03 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[3] at stats at BasicQueryCassandra.scala:18), which has no missing parents
17/04/09 15:04:04 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 7.9 KB, free 332.1 MB)
17/04/09 15:04:04 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 4.3 KB, free 332.1 MB)
17/04/09 15:04:04 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.43.194:47103 (size: 4.3 KB, free: 332.1 MB)
17/04/09 15:04:04 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:996
17/04/09 15:04:04 INFO DAGScheduler: Submitting 4 missing tasks from ResultStage 0 (MapPartitionsRDD[3] at stats at BasicQueryCassandra.scala:18)
17/04/09 15:04:04 INFO TaskSchedulerImpl: Adding task set 0.0 with 4 tasks
17/04/09 15:04:05 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, NODE_LOCAL, 18968 bytes)
17/04/09 15:04:05 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
17/04/09 15:04:10 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1163 bytes result sent to driver
17/04/09 15:04:10 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, NODE_LOCAL, 16700 bytes)
17/04/09 15:04:10 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
17/04/09 15:04:10 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 5118 ms on localhost (executor driver) (1/4)
17/04/09 15:04:10 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1090 bytes result sent to driver
17/04/09 15:04:10 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, localhost, executor driver, partition 2, NODE_LOCAL, 17296 bytes)
17/04/09 15:04:10 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
17/04/09 15:04:10 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 719 ms on localhost (executor driver) (2/4)
17/04/09 15:04:11 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 1177 bytes result sent to driver
17/04/09 15:04:11 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3, localhost, executor driver, partition 3, NODE_LOCAL, 7490 bytes)
17/04/09 15:04:11 INFO Executor: Running task 3.0 in stage 0.0 (TID 3)
17/04/09 15:04:11 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 529 ms on localhost (executor driver) (3/4)
17/04/09 15:04:11 INFO Executor: Finished task 3.0 in stage 0.0 (TID 3). 1090 bytes result sent to driver
17/04/09 15:04:11 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 111 ms on localhost (executor driver) (4/4)
17/04/09 15:04:11 INFO DAGScheduler: ResultStage 0 (stats at BasicQueryCassandra.scala:18) finished in 6.427 s
17/04/09 15:04:11 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
17/04/09 15:04:11 INFO DAGScheduler: Job 0 finished: stats at BasicQueryCassandra.scala:18, took 8.150213 s
stats (count: 0, mean: 0.000000, stdev: NaN, max: -Infinity, min: Infinity)
17/04/09 15:04:12 INFO SparkContext: Starting job: runJob at RDDFunctions.scala:36
17/04/09 15:04:12 INFO DAGScheduler: Got job 1 (runJob at RDDFunctions.scala:36) with 1 output partitions
17/04/09 15:04:12 INFO DAGScheduler: Final stage: ResultStage 1 (runJob at RDDFunctions.scala:36)
17/04/09 15:04:12 INFO DAGScheduler: Parents of final stage: List()
17/04/09 15:04:12 INFO DAGScheduler: Missing parents: List()
17/04/09 15:04:12 INFO DAGScheduler: Submitting ResultStage 1 (ParallelCollectionRDD[4] at parallelize at BasicQueryCassandra.scala:19), which has no missing parents
17/04/09 15:04:12 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 9.7 KB, free 332.1 MB)
17/04/09 15:04:12 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 4.6 KB, free 332.1 MB)
17/04/09 15:04:12 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.43.194:47103 (size: 4.6 KB, free: 332.1 MB)
17/04/09 15:04:12 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:996
17/04/09 15:04:12 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (ParallelCollectionRDD[4] at parallelize at BasicQueryCassandra.scala:19)
17/04/09 15:04:12 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
17/04/09 15:04:12 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 4, localhost, executor driver, partition 0, PROCESS_LOCAL, 5934 bytes)
17/04/09 15:04:12 INFO Executor: Running task 0.0 in stage 1.0 (TID 4)
17/04/09 15:04:13 INFO TableWriter: Wrote 1 rows to tutorialpoints.kv in 0.847 s.
17/04/09 15:04:13 INFO Executor: Finished task 0.0 in stage 1.0 (TID 4). 1253 bytes result sent to driver
17/04/09 15:04:13 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 4) in 953 ms on localhost (executor driver) (1/1)
17/04/09 15:04:13 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
17/04/09 15:04:13 INFO DAGScheduler: ResultStage 1 (runJob at RDDFunctions.scala:36) finished in 0.958 s
17/04/09 15:04:13 INFO DAGScheduler: Job 1 finished: runJob at RDDFunctions.scala:36, took 0.987858 s
17/04/09 15:04:13 INFO SparkContext: Starting job: runJob at RDDFunctions.scala:36
17/04/09 15:04:13 INFO DAGScheduler: Got job 2 (runJob at RDDFunctions.scala:36) with 1 output partitions
17/04/09 15:04:13 INFO DAGScheduler: Final stage: ResultStage 2 (runJob at RDDFunctions.scala:36)
17/04/09 15:04:13 INFO DAGScheduler: Parents of final stage: List()
17/04/09 15:04:13 INFO DAGScheduler: Missing parents: List()
17/04/09 15:04:13 INFO DAGScheduler: Submitting ResultStage 2 (ParallelCollectionRDD[5] at parallelize at BasicQueryCassandra.scala:22), which has no missing parents
17/04/09 15:04:13 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 9.8 KB, free 332.1 MB)
17/04/09 15:04:13 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 4.6 KB, free 332.1 MB)
17/04/09 15:04:13 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.43.194:47103 (size: 4.6 KB, free: 332.1 MB)
17/04/09 15:04:13 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:996
17/04/09 15:04:13 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (ParallelCollectionRDD[5] at parallelize at BasicQueryCassandra.scala:22)
17/04/09 15:04:13 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks
17/04/09 15:04:13 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 5, localhost, executor driver, partition 0, PROCESS_LOCAL, 5978 bytes)
17/04/09 15:04:13 INFO Executor: Running task 0.0 in stage 2.0 (TID 5)
17/04/09 15:04:13 INFO TableWriter: Wrote 1 rows to tutorialpoints.kv in 0.010 s.
17/04/09 15:04:13 INFO Executor: Finished task 0.0 in stage 2.0 (TID 5). 1093 bytes result sent to driver
17/04/09 15:04:13 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 5) in 31 ms on localhost (executor driver) (1/1)
17/04/09 15:04:13 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
17/04/09 15:04:13 INFO DAGScheduler: ResultStage 2 (runJob at RDDFunctions.scala:36) finished in 0.033 s
17/04/09 15:04:13 INFO DAGScheduler: Job 2 finished: runJob at RDDFunctions.scala:36, took 0.054382 s
17/04/09 15:04:21 INFO CassandraConnector: Disconnected from Cassandra cluster: Test Cluster
17/04/09 15:04:22 INFO SparkContext: Invoking stop() from shutdown hook
17/04/09 15:04:22 INFO SerialShutdownHooks: Successfully executed shutdown hook: Clearing session cache for C* connector
17/04/09 15:04:22 INFO SparkUI: Stopped Spark web UI at http://192.168.43.194:4040
17/04/09 15:04:22 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
17/04/09 15:04:23 INFO MemoryStore: MemoryStore cleared
17/04/09 15:04:23 INFO BlockManager: BlockManager stopped
17/04/09 15:04:23 INFO BlockManagerMaster: BlockManagerMaster stopped
17/04/09 15:04:23 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
17/04/09 15:04:23 INFO SparkContext: Successfully stopped SparkContext
17/04/09 15:04:23 INFO ShutdownHookManager: Shutdown hook called
17/04/09 15:04:23 INFO ShutdownHookManager: Deleting directory /tmp/spark-f7b3ef2d-8596-4285-8144-f1bd03829e98

 
 */

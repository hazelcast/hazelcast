# Speed-Up Partition Migrations

|ℹ️ Since: 3.12| 
|-------------|

## Background

### Description

When partition count becomes higher, migration tasks take too much time
to complete. They are considered slow. For example, with 10 nodes and
12k partitions, migration task throughput is \~50 tasks per second. Our
goal is to increase throughput significantly without breaking safety
guarantees.

## Functional Design

### Summary of Functionality

There won't be any functional or behavioral change.

## User Interaction

### API design and/or Prototypes

There won't be any api or config change.

## Technical Design

When partition count becomes higher and higher, cost of publishing the
new partition state increases dramatically. A partition state update
consists of partition replica array (`PartitionReplica[]`)  and
an `int[partitionCount][7].` With the size proportional to partition
count, size of partition state increases, which increases the cost of
"*serialization → transport → deserialization*" of partition state.

If partition count is high but average data size in partitions is low,
then committing migration and publishing new partition state dominate
most of the total migration time. When data size in partition is high,
such as tens or hundreds of megabytes, then cost of "*serialization →
transport → deserialization*" of partition state becomes negligible.

There are a few steps to eliminate or reduce this cost.

-   **Eliminate sending the whole partition table after each
    migration:**   
    Updated partition table is sent to all cluster members after each
    migration. When partition table is large and/or cluster size (number
    of members) is high, then this partition table broadcasting step
    makes a significant impact on migration time. To reduce this cost,
    instead of partition table, only the recently completed migrations
    will be sent. Members receiving completed migrations list, will
    apply those to their partition state and generate the new partition
    table theirselves. That means, instead of sending the latest state
    to cluster, only the recent change-set will be published.   
      

-   **Reduce partition table update traffic after each migration:**  
    Instead of sending updated partition state (or recently completed
    migrations after applying step above) to all cluster members after
    each migration, completed migrations will be sent only to
    migration source and migration destination inside the migration
    operations. Remaining members receive completed migrations in
    batches asynchronously or with a later migration operation.  
      

-   **Avoid redundant periodic partition table publishing:**  
    Instead of publishing partition table periodically to all members,
    without knowing whether or not they have the latest partition
    table, master will ask if their partition table is stale when
    compared to master's version. If one responds as its partition table
    is stale, master will send the partition table to that specific
    member only. That will avoid redundant network traffic from master
    to whole cluster. Additionally, this periodic task will not work
    when there are ongoing migrations, since migration system will take
    care of publishing partition table and/or updates.  
      

## Testing Criteria

There are already a huge number of tests to verify correctness & safety
of migrations. There are also tests to verify rolling upgrade
compatibility of migrations. They are all expected to pass. 

Additionally, performance effect of these changes will be measured using
Hazelcast Simulator.

## Performance Tests

Initially, a single worker is started to fill some partition data and
then it blocks in `Prepare` phase for expected number of members to join
the cluster. Using `coordinator-remote`, additional members are started
and test continues until all migrations are completed. To measure
performance of migration tasks,
an [`InternalMigrationListener`](https://github.com/hazelcast/hazelcast/blob/master/hazelcast/src/main/java/com/hazelcast/internal/partition/impl/InternalMigrationListener.java)
is registered to the master (initial) member and via listener elapsed
time for each migration is recorded to a histogram. Basically tests
execution steps are:

-   Start 5 agents: provisioner --scale 5

-   Start coordinator session to listen commands from
    coordinator-remote: `coordinator`

-   After coordinator starts listening commands, start initial worker:
    `coordinator-remote worker-start --count 1`

-   Start executing the test:
    `coordinator-remote test-run migration-test.properties`

-   When test execution blocks on waiting expected members to join,
    start additional members:
    `coordinator-remote worker-start --count 4`

-   Test will complete and histogram result will be persisted to disk.  
        

  

Results below will show that, current migration mechanism will get
slower and slower dramatically when partition count and/or member count
increases. But with the optimizations explained above, using speedup
branch, tests show that migrations become independent of partition count
and/or member count. Latency of a single migration remains nearly same.

  

Test property file name should not be `test.properties`, it's
intentionally named as `migration-test.properties`.
Because `test.properties` is default file name and when `coordinator`
starts, it picks that file and starts executing the test if it exists.

  

Set `COORDINATOR_PORT=5000` in `simulator.properties` file. 

  

##### **migration-test.properties**

```
MigrationPerfTest@class=MigrationPerfTest
MigrationPerfTest@threadCount=1
MigrationPerfTest@clusterSize=5
MigrationPerfTest@mapCount=1
MigrationPerfTest@entryCount=10000
MigrationPerfTest@valueSize=1000
```

  

##### **MigrationPerfTest**

```java
import com.hazelcast.core.Cluster;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Random;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;

public class MigrationPerfTest extends HazelcastTest {

    public int clusterSize = 5;
    public int mapCount = 10;
    public int entryCount = 10000;
    public int valueSize = 1024;

    private IMap[] maps;
    private Streamer[] streamers;

    private final MasterMigrationListener migrationListener = new MasterMigrationListener();
    private InternalPartitionServiceImpl partitionService;

    @Setup
    public void setup() {
        maps = new IMap[mapCount];
        streamers = new Streamer[mapCount];
        for (int i = 0; i < streamers.length; i++) {
            IMap<Object, Object> map = targetInstance.getMap(newUnsecureUuidString());
            maps[i] = map;
            streamers[i] = StreamerFactory.getInstance(map);
        }
    }

    @Prepare(global = true)
    public void prepare() {
        Random rand = new Random();
        for (int i = 0; i < entryCount; i++) {
            for (Streamer streamer : streamers) {
                byte[] value = new byte[valueSize];
                rand.nextBytes(value);
                streamer.pushEntry(i, value);
            }
        }
        for (Streamer streamer : streamers) {
            streamer.await();
        }
        testContext.echoCoordinator("Inserted %d entries into %d different maps.", entryCount, mapCount);

        Node node = HazelcastTestUtils.getNode(targetInstance);
        if (node.getClusterService().isMaster()) {
            partitionService = node.partitionService;
            partitionService.setInternalMigrationListener(migrationListener);
        }

        waitForCluster();
    }

    private void waitForCluster() {
        testContext.echoCoordinator("Waiting for %d members.", clusterSize);
        Cluster cluster = targetInstance.getCluster();
        while (cluster.getMembers().size() < clusterSize) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Run
    public void migrations() {
        while (!partitionService.isMemberStateSafe()) {
            testContext.echoCoordinator("Remaining migrations: %d", partitionService.getMigrationQueueSize());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        testContext.echoCoordinator("All migrations are completed.");

        try (PrintStream stream = new PrintStream(new File(testContext.getTestId() + "-histogram.hdr"))) {
            HistogramLogWriter writer = new HistogramLogWriter(stream);
            writer.outputIntervalHistogram(migrationListener.histogram);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        testContext.stop();
    }

    @Teardown
    public void tearDown() {
        for (IMap map : maps) {
            map.destroy();
        }
    }

    private class MasterMigrationListener extends InternalMigrationListener {
        // InternalMigrationListener is used by single thread on master member.
        private final Histogram histogram = new Histogram(3);
        private long startTime = -1;

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (participant != MigrationParticipant.MASTER) {
                return;
            }

            if (startTime == -1) {
                startTime = System.nanoTime();
                return;
            }

            histogram.recordValue(System.nanoTime() - startTime);
            startTime = System.nanoTime();
        }
    }
}
```

  

### Test Case 1: 5 Members - 1999 Partitions

On master branch, 7996 migrations are completed in **39 seconds** but on
speedup branch they are completed in **12 seconds**. 

![](resources/05/2372173937.png?height=400)

#### Master branch

Mean latency is 4778 μs, 99th percentile is 7610 μs.  

```
       Value(μs) Percentile TotalCount 1/(1-Percentile)

    3452.927 0.000000000000          1           1.00
    4026.367 0.100000000000        807           1.11
    4222.975 0.200000000000       1599           1.25
    4390.911 0.300000000000       2404           1.43
    4546.559 0.400000000000       3208           1.67
    4669.439 0.500000000000       4002           2.00
    4734.975 0.550000000000       4413           2.22
    4804.607 0.600000000000       4804           2.50
    4878.335 0.650000000000       5200           2.86
    4947.967 0.700000000000       5600           3.33
    5013.503 0.750000000000       6000           4.00
    5054.463 0.775000000000       6214           4.44
    5095.423 0.800000000000       6410           5.00
    5136.383 0.825000000000       6597           5.71
    5193.727 0.850000000000       6801           6.67
    5255.167 0.875000000000       7008           8.00
    5296.127 0.887500000000       7098           8.89
    5337.087 0.900000000000       7205          10.00
    5378.047 0.912500000000       7304          11.43
    5427.199 0.925000000000       7398          13.33
    5492.735 0.937500000000       7497          16.00
    5521.407 0.943750000000       7552          17.78
    5558.271 0.950000000000       7600          20.00
    5603.327 0.956250000000       7648          22.86
    5656.575 0.962500000000       7698          26.67
    5730.303 0.968750000000       7747          32.00
    5779.455 0.971875000000       7771          35.56
    5824.511 0.975000000000       7796          40.00
    5918.719 0.978125000000       7821          45.71
    6017.023 0.981250000000       7846          53.33
    6332.415 0.984375000000       7871          64.00
    6443.007 0.985937500000       7883          71.11
    6770.687 0.987500000000       7896          80.00
    7159.807 0.989062500000       7908          91.43
    7610.367 0.990625000000       7921         106.67
    8052.735 0.992187500000       7933         128.00
    8429.567 0.992968750000       7939         142.22
    8798.207 0.993750000000       7946         160.00
    9379.839 0.994531250000       7952         182.86
   10264.575 0.995312500000       7958         213.33
   10747.903 0.996093750000       7964         256.00
   10813.439 0.996484375000       7967         284.44
   11001.855 0.996875000000       7971         320.00
   11419.647 0.997265625000       7974         365.71
   12156.927 0.997656250000       7978         426.67
   13459.455 0.998046875000       7980         512.00
   14073.855 0.998242187500       7981         568.89
   14532.607 0.998437500000       7983         640.00
   14909.439 0.998632812500       7985         731.43
   14942.207 0.998828125000       7986         853.33
   16080.895 0.999023437500       7988        1024.00
   16080.895 0.999121093750       7988        1137.78
   16252.927 0.999218750000       7989        1280.00
   17842.175 0.999316406250       7990        1462.86
   26771.455 0.999414062500       7991        1706.67
   79495.167 0.999511718750       7992        2048.00
   79495.167 0.999560546875       7992        2275.56
   79495.167 0.999609375000       7992        2560.00
   85000.191 0.999658203125       7993        2925.71
   85000.191 0.999707031250       7993        3413.33
  115867.647 0.999755859375       7994        4096.00
  115867.647 0.999780273438       7994        4551.11
  115867.647 0.999804687500       7994        5120.00
  115867.647 0.999829101563       7994        5851.43
  115867.647 0.999853515625       7994        6826.67
  116326.399 0.999877929688       7995        8192.00
  116326.399 1.000000000000       7995
#[Mean    =     4778.479, StdDeviation   =     2292.453]
#[Max     =   116326.399, Total count    =         7995]
#[Buckets =           17, SubBuckets     =         2048]
```

  

```
13:55:35,006 Re-partitioning cluster data... Migration queue size: 7996
13:55:39,865 Remaining migration tasks in queue => 7085
13:55:44,864 Remaining migration tasks in queue => 5946
13:55:49,865 Remaining migration tasks in queue => 4831
13:55:54,864 Remaining migration tasks in queue => 3724
13:55:59,864 Remaining migration tasks in queue => 2692
13:56:04,864 Remaining migration tasks in queue => 1663
13:56:09,865 Remaining migration tasks in queue => 679
13:56:14,233 All migration tasks have been completed, queues are empty.

```

  

#### Speedup branch

Mean latency is 1325 μs, 99th percentile is 3174 μs. 

```
       Value(μs) Percentile TotalCount 1/(1-Percentile)

     645.631 0.000000000000          1           1.00
     801.279 0.100000000000        801           1.11
     860.159 0.200000000000       1603           1.25
     942.079 0.300000000000       2402           1.43
    1067.007 0.400000000000       3205           1.67
    1199.103 0.500000000000       3998           2.00
    1256.447 0.550000000000       4406           2.22
    1312.767 0.600000000000       4805           2.50
    1362.943 0.650000000000       5207           2.86
    1421.311 0.700000000000       5603           3.33
    1478.655 0.750000000000       5998           4.00
    1513.471 0.775000000000       6204           4.44
    1550.335 0.800000000000       6396           5.00
    1592.319 0.825000000000       6600           5.71
    1640.447 0.850000000000       6801           6.67
    1693.695 0.875000000000       6996           8.00
    1722.367 0.887500000000       7096           8.89
    1767.423 0.900000000000       7198          10.00
    1816.575 0.912500000000       7297          11.43
    1880.063 0.925000000000       7396          13.33
    1970.175 0.937500000000       7496          16.00
    2020.351 0.943750000000       7548          17.78
    2079.743 0.950000000000       7599          20.00
    2138.111 0.956250000000       7646          22.86
    2234.367 0.962500000000       7696          26.67
    2351.103 0.968750000000       7746          32.00
    2435.071 0.971875000000       7773          35.56
    2514.943 0.975000000000       7796          40.00
    2605.055 0.978125000000       7821          45.71
    2725.887 0.981250000000       7846          53.33
    2856.959 0.984375000000       7871          64.00
    2899.967 0.985937500000       7883          71.11
    2959.359 0.987500000000       7896          80.00
    3061.759 0.989062500000       7908          91.43
    3174.399 0.990625000000       7921         106.67
    3301.375 0.992187500000       7933         128.00
    3358.719 0.992968750000       7939         142.22
    3614.719 0.993750000000       7946         160.00
    3676.159 0.994531250000       7952         182.86
    4118.527 0.995312500000       7958         213.33
    4771.839 0.996093750000       7964         256.00
    5009.407 0.996484375000       7967         284.44
    6053.887 0.996875000000       7971         320.00
    7028.735 0.997265625000       7974         365.71
    7872.511 0.997656250000       7977         426.67
    8089.599 0.998046875000       7980         512.00
    8642.559 0.998242187500       7981         568.89
    9674.751 0.998437500000       7983         640.00
   10223.615 0.998632812500       7985         731.43
   10780.671 0.998828125000       7986         853.33
   15720.447 0.999023437500       7988        1024.00
   15720.447 0.999121093750       7988        1137.78
   17711.103 0.999218750000       7989        1280.00
   19546.111 0.999316406250       7990        1462.86
   20217.855 0.999414062500       7991        1706.67
   74514.431 0.999511718750       7992        2048.00
   74514.431 0.999560546875       7992        2275.56
   74514.431 0.999609375000       7992        2560.00
   79691.775 0.999658203125       7993        2925.71
   79691.775 0.999707031250       7993        3413.33
   88014.847 0.999755859375       7994        4096.00
   88014.847 0.999780273438       7994        4551.11
   88014.847 0.999804687500       7994        5120.00
   88014.847 0.999829101563       7994        5851.43
   88014.847 0.999853515625       7994        6826.67
  107216.895 0.999877929688       7995        8192.00
  107216.895 1.000000000000       7995
#[Mean    =     1325.808, StdDeviation   =     2064.082]
#[Max     =   107216.895, Total count    =         7995]
#[Buckets =           17, SubBuckets     =         2048]
```

  

```
13:52:21,962 Re-partitioning cluster data... Migration queue size: 7996
13:52:25,591 Remaining migration tasks in queue => 5928.
13:52:30,587 Remaining migration tasks in queue => 1642.
13:52:33,572 All migration tasks have been completed.
```

  

### Test Case 2: 5 Members - 10k Partitions

On master branch, 40,000 migrations are completed in **11 minutes and 26
seconds** but on speedup branch they are completed in **43 seconds**.  

![](resources/05/2372173934.png?width=265)

  

#### Master branch

Mean latency is 17125 μs, 99th percentile is 22790 μs. 

```
       Value(μs) Percentile TotalCount 1/(1-Percentile)

   11943.935 0.000000000000          1           1.00
   14295.039 0.100000000000       4006           1.11
   15081.471 0.200000000000       8073           1.25
   15515.647 0.300000000000      12011           1.43
   16269.311 0.400000000000      16007           1.67
   17022.975 0.500000000000      20030           2.00
   17481.727 0.550000000000      22055           2.22
   17842.175 0.600000000000      24055           2.50
   18169.855 0.650000000000      26021           2.86
   18481.151 0.700000000000      28030           3.33
   18825.215 0.750000000000      30060           4.00
   18989.055 0.775000000000      31068           4.44
   19136.511 0.800000000000      32105           5.00
   19267.583 0.825000000000      33028           5.71
   19415.039 0.850000000000      34059           6.67
   19562.495 0.875000000000      35086           8.00
   19628.031 0.887500000000      35507           8.89
   19709.951 0.900000000000      36017          10.00
   19808.255 0.912500000000      36579          11.43
   19906.559 0.925000000000      37039          13.33
   20070.399 0.937500000000      37529          16.00
   20185.087 0.943750000000      37758          17.78
   20381.695 0.950000000000      38018          20.00
   20643.839 0.956250000000      38258          22.86
   20938.751 0.962500000000      38502          26.67
   21266.431 0.968750000000      38760          32.00
   21397.503 0.971875000000      38882          35.56
   21594.111 0.975000000000      39001          40.00
   21856.255 0.978125000000      39137          45.71
   22069.247 0.981250000000      39258          53.33
   22282.239 0.984375000000      39379          64.00
   22380.543 0.985937500000      39441          71.11
   22478.847 0.987500000000      39503          80.00
   22626.303 0.989062500000      39562          91.43
   22790.143 0.990625000000      39630         106.67
   22953.983 0.992187500000      39687         128.00
   23068.671 0.992968750000      39719         142.22
   23232.511 0.993750000000      39751         160.00
   23396.351 0.994531250000      39782         182.86
   23691.263 0.995312500000      39813         213.33
   24150.015 0.996093750000      39843         256.00
   24412.159 0.996484375000      39859         284.44
   24657.919 0.996875000000      39875         320.00
   25133.055 0.997265625000      39892         365.71
   25772.031 0.997656250000      39906         426.67
   26771.455 0.998046875000      39921         512.00
   27213.823 0.998242187500      39929         568.89
   27672.575 0.998437500000      39937         640.00
   28606.463 0.998632812500      39945         731.43
   29360.127 0.998828125000      39953         853.33
   30834.687 0.999023437500      39960        1024.00
   31965.183 0.999121093750      39964        1137.78
   32849.919 0.999218750000      39968        1280.00
   33456.127 0.999316406250      39972        1462.86
   34373.631 0.999414062500      39976        1706.67
   37355.519 0.999511718750      39980        2048.00
   39026.687 0.999560546875      39982        2275.56
   41811.967 0.999609375000      39984        2560.00
   43941.887 0.999658203125      39986        2925.71
   44597.247 0.999707031250      39988        3413.33
   59736.063 0.999755859375      39990        4096.00
   62554.111 0.999780273438      39991        4551.11
   90308.607 0.999804687500      39992        5120.00
   95551.487 0.999829101563      39993        5851.43
  101056.511 0.999853515625      39994        6826.67
  102694.911 0.999877929688      39995        8192.00
  102694.911 0.999890136719      39995        9102.22
  136577.023 0.999902343750      39996       10240.00
  136577.023 0.999914550781      39996       11702.86
  140115.967 0.999926757813      39997       13653.33
  140115.967 0.999938964844      39997       16384.00
  140115.967 0.999945068359      39997       18204.44
  147587.071 0.999951171875      39998       20480.00
  147587.071 0.999957275391      39998       23405.71
  147587.071 0.999963378906      39998       27306.67
  147587.071 0.999969482422      39998       32768.00
  147587.071 0.999972534180      39998       36408.89
  174981.119 0.999975585938      39999       40960.00
  174981.119 1.000000000000      39999
#[Mean    =    17124.954, StdDeviation   =     2778.581]
#[Max     =   174981.119, Total count    =        39999]
#[Buckets =           18, SubBuckets     =         2048]
```

  

``` java
07:57:50,025 Re-partitioning cluster data... Migration queue size: 40000
07:57:53,631 Remaining migration tasks in queue => 39807
...
07:59:48,629 Remaining migration tasks in queue => 31970
...
08:02:38,629 Remaining migration tasks in queue => 21228
...
08:03:33,629 Remaining migration tasks in queue => 18008
...
08:04:58,629 Remaining migration tasks in queue => 13284
...
08:06:13,629 Remaining migration tasks in queue => 9258
...
08:08:03,629 Remaining migration tasks in queue => 3607
...
08:08:43,629 Remaining migration tasks in queue => 1596
...
08:09:03,629 Remaining migration tasks in queue => 579
08:09:16,077 All migration tasks have been completed, queues are empty.
```

  

#### Speedup branch

Mean latency is 1052 μs, 99th percentile is 2332 μs. 

```
       Value(μs) Percentile TotalCount 1/(1-Percentile)

     575.999 0.000000000000          1           1.00
     679.935 0.100000000000       4011           1.11
     724.991 0.200000000000       8006           1.25
     791.551 0.300000000000      12000           1.43
     904.191 0.400000000000      16012           1.67
     991.743 0.500000000000      20023           2.00
    1030.655 0.550000000000      22003           2.22
    1063.935 0.600000000000      24034           2.50
    1097.727 0.650000000000      26051           2.86
    1131.519 0.700000000000      28000           3.33
    1173.503 0.750000000000      30024           4.00
    1196.031 0.775000000000      31032           4.44
    1220.607 0.800000000000      32031           5.00
    1249.279 0.825000000000      33025           5.71
    1282.047 0.850000000000      34032           6.67
    1318.911 0.875000000000      35022           8.00
    1339.391 0.887500000000      35502           8.89
    1367.039 0.900000000000      36002          10.00
    1396.735 0.912500000000      36500          11.43
    1429.503 0.925000000000      37001          13.33
    1473.535 0.937500000000      37503          16.00
    1496.063 0.943750000000      37751          17.78
    1529.855 0.950000000000      38002          20.00
    1565.695 0.956250000000      38254          22.86
    1610.751 0.962500000000      38503          26.67
    1675.263 0.968750000000      38751          32.00
    1718.271 0.971875000000      38876          35.56
    1768.447 0.975000000000      39002          40.00
    1825.791 0.978125000000      39129          45.71
    1895.423 0.981250000000      39251          53.33
    1998.847 0.984375000000      39375          64.00
    2054.143 0.985937500000      39438          71.11
    2129.919 0.987500000000      39501          80.00
    2226.175 0.989062500000      39565          91.43
    2332.671 0.990625000000      39625         106.67
    2461.695 0.992187500000      39688         128.00
    2537.471 0.992968750000      39718         142.22
    2625.535 0.993750000000      39750         160.00
    2760.703 0.994531250000      39781         182.86
    2969.599 0.995312500000      39812         213.33
    3180.543 0.996093750000      39843         256.00
    3336.191 0.996484375000      39859         284.44
    3594.239 0.996875000000      39875         320.00
    3883.007 0.997265625000      39890         365.71
    4263.935 0.997656250000      39906         426.67
    5079.039 0.998046875000      39921         512.00
    5660.671 0.998242187500      39929         568.89
    7151.615 0.998437500000      39937         640.00
    8445.951 0.998632812500      39945         731.43
   10289.151 0.998828125000      39953         853.33
   12173.311 0.999023437500      39960        1024.00
   13385.727 0.999121093750      39964        1137.78
   18317.311 0.999218750000      39968        1280.00
   20955.135 0.999316406250      39972        1462.86
   23592.959 0.999414062500      39976        1706.67
   26132.479 0.999511718750      39981        2048.00
   27377.663 0.999560546875      39982        2275.56
   28147.711 0.999609375000      39984        2560.00
   28229.631 0.999658203125      39986        2925.71
   30261.247 0.999707031250      39988        3413.33
   31244.287 0.999755859375      39990        4096.00
   31719.423 0.999780273438      39991        4551.11
   31850.495 0.999804687500      39992        5120.00
   31965.183 0.999829101563      39994        5851.43
   31965.183 0.999853515625      39994        6826.67
   34635.775 0.999877929688      39995        8192.00
   34635.775 0.999890136719      39995        9102.22
   84606.975 0.999902343750      39996       10240.00
   84606.975 0.999914550781      39996       11702.86
   88014.847 0.999926757813      39997       13653.33
   88014.847 0.999938964844      39997       16384.00
   88014.847 0.999945068359      39997       18204.44
   89194.495 0.999951171875      39998       20480.00
   89194.495 0.999957275391      39998       23405.71
   89194.495 0.999963378906      39998       27306.67
   89194.495 0.999969482422      39998       32768.00
   89194.495 0.999972534180      39998       36408.89
  103415.807 0.999975585938      39999       40960.00
  103415.807 1.000000000000      39999
#[Mean    =     1052.031, StdDeviation   =     1216.413]
#[Max     =   103415.807, Total count    =        39999]
#[Buckets =           17, SubBuckets     =         2048]
```

  

```
07:53:06,872 Re-partitioning cluster data... Migration queue size: 40000
07:53:10,146 Remaining migration tasks in queue => 38273.
07:53:15,142 Remaining migration tasks in queue => 34287. 
07:53:20,142 Remaining migration tasks in queue => 29614. 
07:53:25,142 Remaining migration tasks in queue => 24837. 
07:53:30,142 Remaining migration tasks in queue => 19375. 
07:53:35,147 Remaining migration tasks in queue => 14468. 
07:53:40,142 Remaining migration tasks in queue => 9078. 
07:53:45,142 Remaining migration tasks in queue => 4300. 
07:53:49,980 All migration tasks have been completed. 
```

  

### Test Case 3: 10 Members - 10k Partitions

On master branch, 63000 migrations are completed in **31 minutes and 56
seconds** but on speedup branch they are completed in **1 minute and 7
seconds**.  

![](resources/05/2372173931.png?width=265)

#### Master branch

Mean latency is 30383 μs, 99th percentile is 34603 μs. 

```
       Value(μs) Percentile TotalCount 1/(1-Percentile)

   16211.967 0.000000000000          1           1.00
   29655.039 0.100000000000       6483           1.11
   30146.559 0.200000000000      13451           1.25
   30195.711 0.300000000000      19661           1.43
   30228.479 0.400000000000      26280           1.67
   30261.247 0.500000000000      34346           2.00
   30277.631 0.550000000000      38022           2.22
   30277.631 0.600000000000      38022           2.50
   30294.015 0.650000000000      41236           2.86
   30326.783 0.700000000000      46237           3.33
   30343.167 0.750000000000      48043           4.00
   30359.551 0.775000000000      49432           4.44
   30375.935 0.800000000000      50502           5.00
   30425.087 0.825000000000      52419           5.71
   30490.623 0.850000000000      53618           6.67
   30736.383 0.875000000000      55160           8.00
   30851.071 0.887500000000      56068           8.89
   30916.607 0.900000000000      56808          10.00
   30998.527 0.912500000000      57576          11.43
   31211.519 0.925000000000      58280          13.33
   31555.583 0.937500000000      59071          16.00
   31768.575 0.943750000000      59483          17.78
   31899.647 0.950000000000      59880          20.00
   31997.951 0.956250000000      60253          22.86
   32161.791 0.962500000000      60669          26.67
   32489.471 0.968750000000      61047          32.00
   32718.847 0.971875000000      61228          35.56
   32882.687 0.975000000000      61449          40.00
   32948.223 0.978125000000      61633          45.71
   33013.759 0.981250000000      61850          53.33
   33095.679 0.984375000000      62038          64.00
   33161.215 0.985937500000      62132          71.11
   33292.287 0.987500000000      62212          80.00
   33554.431 0.989062500000      62310          91.43
   34603.007 0.990625000000      62410         106.67
   39747.583 0.992187500000      62507         128.00
   42598.399 0.992968750000      62557         142.22
   45252.607 0.993750000000      62607         160.00
   48398.335 0.994531250000      62655         182.86
   51773.439 0.995312500000      62704         213.33
   56393.727 0.996093750000      62753         256.00
   57147.391 0.996484375000      62778         284.44
   57409.535 0.996875000000      62804         320.00
   57475.071 0.997265625000      62831         365.71
   57540.607 0.997656250000      62860         426.67
   57606.143 0.998046875000      62881         512.00
   57638.911 0.998242187500      62891         568.89
   57868.287 0.998437500000      62901         640.00
   58490.879 0.998632812500      62913         731.43
   59342.847 0.998828125000      62926         853.33
   60751.871 0.999023437500      62938        1024.00
   63143.935 0.999121093750      62944        1137.78
   65110.015 0.999218750000      62950        1280.00
   68616.191 0.999316406250      62957        1462.86
   70975.487 0.999414062500      62963        1706.67
   73138.175 0.999511718750      62969        2048.00
   74252.287 0.999560546875      62972        2275.56
   75366.399 0.999609375000      62975        2560.00
   76021.759 0.999658203125      62978        2925.71
   78577.663 0.999707031250      62981        3413.33
   87687.167 0.999755859375      62984        4096.00
   93585.407 0.999780273438      62986        4551.11
   95879.167 0.999804687500      62987        5120.00
   97386.495 0.999829101563      62989        5851.43
  101449.727 0.999853515625      62990        6826.67
  105447.423 0.999877929688      62992        8192.00
  106299.391 0.999890136719      62993        9102.22
  106299.391 0.999902343750      62993       10240.00
  107937.791 0.999914550781      62994       11702.86
  110100.479 0.999926757813      62995       13653.33
  113442.815 0.999938964844      62996       16384.00
  113442.815 0.999945068359      62996       18204.44
  113442.815 0.999951171875      62996       20480.00
  123928.575 0.999957275391      62997       23405.71
  123928.575 0.999963378906      62997       27306.67
  156237.823 0.999969482422      62998       32768.00
  156237.823 0.999972534180      62998       36408.89
  156237.823 0.999975585938      62998       40960.00
  156237.823 0.999978637695      62998       46811.43
  156237.823 0.999981689453      62998       54613.33
  316145.663 0.999984741211      62999       65536.00
  316145.663 1.000000000000      62999
#[Mean    =    30383.098, StdDeviation   =     2972.490]
#[Max     =   316145.663, Total count    =        62999]
#[Buckets =           19, SubBuckets     =         2048]
```

  

```
14:13:53,957 Re-partitioning cluster data... Migration queue size: 63000
14:13:56,795 Remaining migration tasks in queue => 62914
...
14:16:06,795 Remaining migration tasks in queue => 58530
...
14:20:46,794 Remaining migration tasks in queue => 49331
...
14:27:46,795 Remaining migration tasks in queue => 35530
...
14:31:41,795 Remaining migration tasks in queue => 27809
...
14:36:06,794 Remaining migration tasks in queue => 19102
...
14:40:46,795 Remaining migration tasks in queue => 9902
...
14:44:16,794 Remaining migration tasks in queue => 3002
...
14:45:41,795 Remaining migration tasks in queue => 211
14:45:49,183 All migration tasks have been completed, queues are empty.
```

  

#### Speedup branch

Mean latency is 1041 μs, 99th percentile is 2322 μs. 

```
       Value(μs) Percentile TotalCount 1/(1-Percentile)

     583.167 0.000000000000          1           1.00
     685.055 0.100000000000       6381           1.11
     715.263 0.200000000000      12700           1.25
     756.223 0.300000000000      18919           1.43
     823.807 0.400000000000      25226           1.67
     922.111 0.500000000000      31521           2.00
     983.039 0.550000000000      34649           2.22
    1041.919 0.600000000000      37825           2.50
    1092.607 0.650000000000      41010           2.86
    1140.735 0.700000000000      44109           3.33
    1192.959 0.750000000000      47303           4.00
    1223.679 0.775000000000      48869           4.44
    1255.423 0.800000000000      50402           5.00
    1291.263 0.825000000000      51985           5.71
    1331.199 0.850000000000      53553           6.67
    1376.255 0.875000000000      55132           8.00
    1404.927 0.887500000000      55928           8.89
    1435.647 0.900000000000      56727          10.00
    1470.463 0.912500000000      57499          11.43
    1510.399 0.925000000000      58291          13.33
    1559.551 0.937500000000      59066          16.00
    1590.271 0.943750000000      59465          17.78
    1623.039 0.950000000000      59859          20.00
    1655.807 0.956250000000      60243          22.86
    1702.911 0.962500000000      60642          26.67
    1750.015 0.968750000000      61030          32.00
    1788.927 0.971875000000      61232          35.56
    1828.863 0.975000000000      61426          40.00
    1881.087 0.978125000000      61624          45.71
    1952.767 0.981250000000      61818          53.33
    2031.615 0.984375000000      62014          64.00
    2091.007 0.985937500000      62113          71.11
    2152.447 0.987500000000      62213          80.00
    2244.607 0.989062500000      62309          91.43
    2322.431 0.990625000000      62409         106.67
    2447.359 0.992187500000      62508         128.00
    2510.847 0.992968750000      62556         142.22
    2580.479 0.993750000000      62606         160.00
    2668.543 0.994531250000      62654         182.86
    2799.615 0.995312500000      62703         213.33
    2961.407 0.996093750000      62752         256.00
    3094.527 0.996484375000      62778         284.44
    3252.223 0.996875000000      62802         320.00
    3457.023 0.997265625000      62826         365.71
    3907.583 0.997656250000      62851         426.67
    4403.199 0.998046875000      62875         512.00
    4825.087 0.998242187500      62888         568.89
    5500.927 0.998437500000      62900         640.00
    7028.735 0.998632812500      62912         731.43
    7856.127 0.998828125000      62925         853.33
    8527.871 0.999023437500      62937        1024.00
    9355.263 0.999121093750      62943        1137.78
   10690.559 0.999218750000      62949        1280.00
   12746.751 0.999316406250      62955        1462.86
   18726.911 0.999414062500      62962        1706.67
   20267.007 0.999511718750      62968        2048.00
   20807.679 0.999560546875      62971        2275.56
   22265.855 0.999609375000      62974        2560.00
   25083.903 0.999658203125      62977        2925.71
   28770.303 0.999707031250      62980        3413.33
   31064.063 0.999755859375      62983        4096.00
   34668.543 0.999780273438      62985        4551.11
   40959.999 0.999804687500      62986        5120.00
   42991.615 0.999829101563      62988        5851.43
   49741.823 0.999853515625      62989        6826.67
   79364.095 0.999877929688      62991        8192.00
   81788.927 0.999890136719      62992        9102.22
   81788.927 0.999902343750      62992       10240.00
   83296.255 0.999914550781      62993       11702.86
   83558.399 0.999926757813      62994       13653.33
   87031.807 0.999938964844      62995       16384.00
   87031.807 0.999945068359      62995       18204.44
   87031.807 0.999951171875      62995       20480.00
   87293.951 0.999957275391      62996       23405.71
   87293.951 0.999963378906      62996       27306.67
   97386.495 0.999969482422      62997       32768.00
   97386.495 0.999972534180      62997       36408.89
   97386.495 0.999975585938      62997       40960.00
   97386.495 0.999978637695      62997       46811.43
   97386.495 0.999981689453      62997       54613.33
  108593.151 0.999984741211      62998       65536.00
  108593.151 1.000000000000      62998
#[Mean    =     1041.657, StdDeviation   =     1264.536]
#[Max     =   108593.151, Total count    =        62998]
#[Buckets =           17, SubBuckets     =         2048]
```

  

```
14:49:42,935 Re-partitioning cluster data... Migration queue size: 62999
14:49:46,365 Remaining migration tasks in queue => 61700. 
14:49:51,361 Remaining migration tasks in queue => 58138. 
14:49:56,361 Remaining migration tasks in queue => 54335. 
14:50:01,361 Remaining migration tasks in queue => 49489. 
14:50:06,361 Remaining migration tasks in queue => 43913. 
14:50:11,361 Remaining migration tasks in queue => 38227. 
14:50:16,361 Remaining migration tasks in queue => 32725. 
14:50:21,361 Remaining migration tasks in queue => 27510. 
14:50:26,361 Remaining migration tasks in queue => 21986. 
14:50:31,361 Remaining migration tasks in queue => 16945. 
14:50:36,361 Remaining migration tasks in queue => 11902. 
14:50:41,361 Remaining migration tasks in queue => 7023. 
14:50:46,361 Remaining migration tasks in queue => 2054. 
14:50:49,596 All migration tasks have been completed. 
```


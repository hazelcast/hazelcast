package com.hazelcast.benchmarks;

import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.concurrent.Future;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-remoteoperation")
@BenchmarkHistoryChart(filePrefix = "benchmark-remoteoperation-history", labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class RemoteOperationBenchmark extends HazelcastTestSupport {

    @Test
    public void remoteInvokeOnPartition() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        HazelcastTestSupport.warmUpPartitions(instances);

        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        int partitionId = findAnyAssignedPartition(remote);
        long startMs = System.currentTimeMillis();

        long iterations = 300 * 1000 * 1000;

        Future[] futures = new Future[1000];
        int futureIndex = 0;
        for (long k = 0; k < iterations; k++) {
            DummyOperation op = new DummyOperation();
            futures[futureIndex]= operationService.invokeOnPartition(null, op, partitionId);
            futureIndex++;

            if(futureIndex==futures.length){
                for(Future f: futures){
                    f.get();
                }
                futureIndex=0;
            }

            if(k%10000 == 0){
                System.out.println("At "+k);
            }
        }

        System.out.println("Successfully retrieved data from remote partition");

        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Performance: " + String.format("%1$,.2f", performance));
    }

    private int findAnyAssignedPartition(HazelcastInstance hz) {
        for (Partition p : hz.getPartitionService().getPartitions()) {
            Member owner = p.getOwner();
            if (owner == null) {
                continue;
            }
            if (owner.localMember()) {
                return p.getPartitionId();
            }
        }

        throw new RuntimeException("No owned partition found");
    }

    public static void main(String[] args) throws Exception {
        RemoteOperationBenchmark benchmark = new RemoteOperationBenchmark();
        benchmark.remoteInvokeOnPartition();
    }

    public static class DummyOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return new Integer(10);
        }
    }
}

package com.hazelcast.benchmarks;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.IOException;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-dataserializable")
@BenchmarkHistoryChart(filePrefix = "benchmark-dataserializable-history", labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class DataSerializableBenchmark extends HazelcastTestSupport {

    private static SerializationService serializationService;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() {
        hazelcastInstance = Hazelcast.newHazelcastInstance();
        serializationService = getNode(hazelcastInstance).getSerializationService();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void serialize() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 100000000;

        DataSerializable object = new DataSerializableObject();
        for (int k = 0; k < iterations; k++) {
            if (k % 5000000 == 0) {
                System.out.println("At: " + k);
            }
            Data data = serializationService.toData(object);
            if (data == null) {
                throw new NullPointerException();
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Serialize DataSerializable performance: " + performance);
    }

    @Test
    public void deserialize() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 100000000;

        DataSerializable object = new DataSerializableObject();
        Data data = serializationService.toData(object);
        for (int k = 0; k < iterations; k++) {
            if (k % 5000000 == 0) {
                System.out.println("At: " + k);
            }
            Object x = serializationService.toObject(data);
            if (x == null) {
                throw new NullPointerException();
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Deserialize DataSerializable performance: " + performance);
    }

    private static class DataSerializableObject implements DataSerializable {
        private int field;

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(field);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            field = in.readInt();
        }
    }

    public static void main(String[] args) throws Exception {
        DataSerializableBenchmark.beforeClass();
        DataSerializableBenchmark benchmark = new DataSerializableBenchmark();
        benchmark.serialize();
        AtomicLongBenchmark.afterClass();
    }
}

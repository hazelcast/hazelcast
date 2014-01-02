package com.hazelcast.benchmarks;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.IOException;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-identifieddataserializable")
@BenchmarkHistoryChart(filePrefix = "benchmark-identifieddataserializable-history", labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class IdentifiedDataSerializableBenchmark extends HazelcastTestSupport {

    private static SerializationService serializationService;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(1, new DummyObjectFactory());
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        serializationService = getNode(hazelcastInstance).getSerializationService();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void serialize() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 100 * 1000 * 1000;

        DataSerializable object = new DummyObject();
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
        System.out.println("Serialize IdentifiedDataSerializable performance: " + performance);
    }

    @Test
    public void deserialize() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 100 * 1000 * 1000;

        DummyObject object = new DummyObject();
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
        System.out.println("Deserialize IdentifiedDataSerializable performance: " + performance);
    }

    private static class DummyObjectFactory implements DataSerializableFactory {
        private static final int F_ID = 1;

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return new DummyObject();
        }
    }

    private static class DummyObject implements IdentifiedDataSerializable {
        private int field;

        @Override
        public int getFactoryId() {
            return DummyObjectFactory.F_ID;
        }

        @Override
        public int getId() {
            return 1;
        }

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
        IdentifiedDataSerializableBenchmark.beforeClass();
        IdentifiedDataSerializableBenchmark benchmark = new IdentifiedDataSerializableBenchmark();
        benchmark.serialize();
        benchmark.serialize();
        benchmark.serialize();
        AtomicLongBenchmark.afterClass();
    }
}


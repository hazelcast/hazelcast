package com.hazelcast.benchmarks;


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.*;
import org.junit.rules.TestRule;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-map")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class ExampleBenchmark {
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private static HazelcastInstance hazelcastInstance;
    private IMap<Object, Object> map;

    @BeforeClass
    public static void beforeClass() {
        hazelcastInstance = Hazelcast.newHazelcastInstance();
    }

    @Before
    public void before(){
        map = hazelcastInstance.getMap("exampleMap");
    }

    @After
    public void after(){
        map.destroy();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @BenchmarkOptions
    @Test
    public void get() throws Exception {
        for(int k=0;k<100000;k++){
            map.get("foo");
        }
    }

    @BenchmarkOptions
    @Test
    public void set() throws Exception {
        for(int k=0;k<100000;k++){
            map.set("foo", "bar");
        }
    }
}

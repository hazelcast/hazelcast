package com.hazelcast.projection;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class QueryBenchmark {
    IMap map;

    @Setup
    public void prepare() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig("persons");
        config.addMapConfig(mapConfig);
        mapConfig.addMapIndexConfig(new MapIndexConfig("age", false));
        mapConfig.addMapIndexConfig(new MapIndexConfig("iq", false));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        map = hz.getMap("persons");
        for (int k = 0; k < 100000; k++) {
            Person person = new Person(k, 100);
            map.put(k, person);
        }
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void testAllIndices() {
        map.keySet(new SqlPredicate("age=10 and iq=100"));
    }

    @Benchmark
    public void testSuppression() {
        map.keySet(new SqlPredicate("age=10 and %iq=100"));
    }

    public static class Person implements Serializable {
        private int iq;
        private int age;

        public Person(int age, int iq) {
            this.age = age;
            this.iq = iq;
        }

        public int getIq() {
            return iq;
        }

        public int getAge() {
            return age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "age=" + age +
                    ", iq=" + iq +
                    '}';
        }
    }

}


/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.spi.properties.GroupProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 5, jvmArgsAppend = {"-Xms4G", "-Xmx4G"})
@State(Scope.Benchmark)
public class MetricsCollectionBenchmark {

    @Param({"1", "10", "100", "1000", "10000"})
    protected int mapCount;

    private HazelcastInstance hazelcastInstance;
    private MetricsService metricsService;

    @Setup
    public void setup() {
        Config config = new Config();
        config.setProperty(GroupProperty.LOGGING_TYPE.getName(), "none");
        config.getMetricsConfig()
              .setMcEnabled(true)
              .setMetricsForDataStructuresEnabled(true)
              // we disable scheduled collection
              .setCollectionIntervalSeconds(Integer.MAX_VALUE)
              .setJmxEnabled(false)
              .setMinimumLevel(DEBUG);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        metricsService = getNodeEngineImpl(hazelcastInstance).getService(MetricsService.SERVICE_NAME);

        for (int i = 0; i < mapCount; i++) {
            hazelcastInstance.getMap("map" + i).put(i, i);
        }
    }

    @TearDown
    public void tearDown() {
        hazelcastInstance.shutdown();
    }

    @Benchmark
    public void collectionWithRecording() {
        metricsService.collectMetrics();
    }

    @Benchmark
    public void pureCollection(Blackhole blackhole) {
        metricsService.collectMetrics(new MetricsCollector() {
            @Override
            public void collectLong(MetricDescriptor descriptor, long value) {
                blackhole.consume(value);
            }

            @Override
            public void collectDouble(MetricDescriptor descriptor, double value) {
                blackhole.consume(value);
            }

            @Override
            public void collectException(MetricDescriptor descriptor, Exception e) {
                blackhole.consume(e);
            }

            @Override
            public void collectNoValue(MetricDescriptor descriptor) {
            }
        });
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MetricsCollectionBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

}

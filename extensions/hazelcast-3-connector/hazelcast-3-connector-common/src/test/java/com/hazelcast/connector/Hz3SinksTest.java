/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

public class Hz3SinksTest extends BaseHz3Test {

    @Test
    public void testMapSink() {
        HazelcastInstance hz = createHazelcastInstance();

        Pipeline p = Pipeline.create();
        BatchSource<SimpleEntry<Integer, String>> source = TestSources.items(
                new SimpleEntry<>(1, "a"),
                new SimpleEntry<>(2, "b")
        );
        Sink<Map.Entry<Integer, String>> sink = Hz3Sinks.remoteMap("test-map", HZ3_CLIENT_CONFIG);
        p.readFrom(source)
                .writeTo(sink);

        JobConfig config = getJobConfig(sink.name());
        hz.getJet().newJob(p, config).join();

        Map<Integer, String> testMap = hz3.getMap("test-map");

        assertThat(testMap).containsOnly(
                entry(1, "a"),
                entry(2, "b")
        );
    }

    @Test
    public void when_writeToInstanceDown_then_shouldThrowJetException() {
        HazelcastInstance hz = createHazelcastInstance();

        Pipeline p = Pipeline.create();
        BatchSource<SimpleEntry<Integer, String>> source = TestSources.items(
                new SimpleEntry<>(1, "a"),
                new SimpleEntry<>(2, "b")
        );
        Sink<Map.Entry<Integer, String>> sink = Hz3Sinks.remoteMap("test-map", HZ3_DOWN_CLIENT_CONFIG);
        p.readFrom(source)
         .writeTo(sink);

        JobConfig config = getJobConfig(sink.name());
        Job job = hz.getJet().newJob(p, config);

        assertThatThrownBy(() -> job.join())
                .hasStackTraceContaining(JetException.class.getName())
                .hasStackTraceContaining("Unable to connect to any cluster");
    }
}

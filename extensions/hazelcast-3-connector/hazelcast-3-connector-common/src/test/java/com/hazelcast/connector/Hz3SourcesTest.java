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

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class Hz3SourcesTest extends BaseHz3Test {

    @Test
    public void readFromEmptyMap() {
        hz3.getMap("test-map"); // make sure the map exists

        HazelcastInstance hz = createHazelcastInstance();

        Pipeline p = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source = Hz3Sources.remoteMap("test-map", HZ3_CLIENT_CONFIG);
        p.readFrom(source)
                .map(Map.Entry::getValue)
                .writeTo(Sinks.list("test-result"));

        JobConfig config = getJobConfig(source.name());
        Job job = hz.getJet().newJob(p, config);

        job.join();

        IList<String> result = hz.getList("test-result");
        assertThat(result).isEmpty();
    }

    @Test
    public void readFromMapKeyValue() {
        IMap<Integer, String> map = hz3.getMap("test-map");

        map.put(42, "hello world");

        HazelcastInstance hz = createHazelcastInstance();

        Pipeline p = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source = Hz3Sources.remoteMap("test-map", HZ3_CLIENT_CONFIG);
        p.readFrom(source)
                .writeTo(Sinks.list("test-result"));

        JobConfig config = getJobConfig(source.name());
        Job job = hz.getJet().newJob(p, config);

        job.join();

        IList<Map.Entry<Integer, String>> result = hz.getList("test-result");
        assertThat(result).contains(entry(42, "hello world"));
    }

    @Test
    public void readFromMapManyItems() {
        IMap<Integer, String> map = hz3.getMap("test-map");

        Map<Integer, String> items = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            items.put(i, "item " + i);
        }
        map.putAll(items);

        HazelcastInstance hz = createHazelcastInstance();

        Pipeline p = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source = Hz3Sources.remoteMap("test-map", HZ3_CLIENT_CONFIG);
        p.readFrom(source)
                .writeTo(Sinks.map("test-result"));

        JobConfig config = getJobConfig(source.name());
        Job job = hz.getJet().newJob(p, config);

        job.join();

        IMap<Integer, String> result = hz.getMap("test-result");
        assertThat(result.entrySet()).isEqualTo(items.entrySet());
    }

}

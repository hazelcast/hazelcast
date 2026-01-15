/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StatefulStageEvictionTest extends PipelineStreamTestSupport {

    @Test
    public void stream_mapStateful_deleteState() {
        int size = 6;
        List<Integer> input = sequence(size);
        StreamStage<Integer> sourceStage = streamStageFromList(input);

        sourceStage.groupingKey(e -> 0)
                .mapStateful(
                        0,
                        () -> new int[1],
                        (state, stateKey, e) -> {
                            state[0] += 1;
                            return state[0];
                        },
                        (state, key, e) -> state[0] == 3,
                        (state, stateKey, wm) -> {
                            throw new RuntimeException("Should not be called");
                        }
                )
                .writeTo(Sinks.list("output"));

        var job = hz().getJet().newJob(p);
        job.join();

        IList<Object> output = member.getList("output");
        assertTrueEventually(() -> assertThat(output).size().isEqualTo(size), 5);
        assertThat(output).containsExactly(1, 2, 3, 1, 2, 3);
    }

    @Test
    public void stream_flatMapStateful_deleteState() {
        int size = 6;
        List<Integer> input = sequence(size);
        StreamStage<Integer> sourceStage = streamStageFromList(input);

        sourceStage.groupingKey(e -> 0)
                .flatMapStateful(
                        0,
                        () -> new int[1],
                        (state, stateKey, e) -> {
                            state[0] += 1;
                            return Traversers.singleton(state[0]);
                        },
                        (state, key, e) -> state[0] == 3,
                        (state, stateKey, wm) -> {
                            throw new RuntimeException("Should not be called");
                        }
                )
                .writeTo(Sinks.list("output"));

        var job = hz().getJet().newJob(p);
        job.join();

        IList<Object> output = member.getList("output");
        assertTrueEventually(() -> assertThat(output).size().isEqualTo(size));
        assertThat(output).containsExactly(1, 2, 3, 1, 2, 3);
    }

    @Test
    public void stream_filterStateful_deleteState() {
        int size = 6;
        List<Integer> input = sequence(size);
        StreamStage<Integer> sourceStage = streamStageFromList(input);

        sourceStage.groupingKey(e -> 0)
                .filterStateful(
                        0,
                        () -> new int[1],
                        (state, e) -> {
                            state[0] += 1;
                            return state[0] == 3;
                        },
                        (state, key, e) -> state[0] == 3
                )
                .writeTo(Sinks.list("output"));

        var job = hz().getJet().newJob(p);
        job.join();

        IList<Integer> output = member.getList("output");
        assertTrueEventually(() -> assertThat(output).size().isEqualTo(2));
        assertThat(output).containsExactly(2, 5);
    }

    @Test
    public void batch_mapStateful_deleteState() {
        int size = 6;
        var input = sequence(size);
        var p = Pipeline.create();
        p.readFrom(TestSources.items(input))
                .groupingKey(e -> 0)
                .mapStateful(
                        () -> new int[1],
                        (state, stateKey, e) -> {
                            state[0] += 1;
                            return state[0];
                        },
                        (state, key, e) -> state[0] == 3
                )
                .writeTo(Sinks.list("output"));

        var job = hz().getJet().newJob(p);
        job.join();

        IList<Object> output = member.getList("output");
        assertThat(output).containsExactly(1, 2, 3, 1, 2, 3);
    }

    @Test
    public void batch_flatMapStateful_deleteState() {
        int size = 6;
        var input = sequence(size);
        var p = Pipeline.create();
        p.readFrom(TestSources.items(input))
                .groupingKey(e -> 0)
                .flatMapStateful(
                        () -> new int[1],
                        (state, stateKey, e) -> {
                            state[0] += 1;
                            return Traversers.singleton(state[0]);
                        },
                        (state, key, e) -> state[0] == 3
                )
                .writeTo(Sinks.list("output"));

        var job = hz().getJet().newJob(p);
        job.join();

        IList<Object> output = member.getList("output");
        assertThat(output).containsExactly(1, 2, 3, 1, 2, 3);
    }

    @Test
    public void batch_filterStateful_deleteState() {
        int size = 6;
        var input = sequence(size);
        var p = Pipeline.create();
        p.readFrom(TestSources.items(input))
                .groupingKey(e -> 0)
                .filterStateful(
                        () -> new int[1],
                        (state, e) -> {
                            state[0] += 1;
                            return state[0] == 3;
                        },
                        (state, key, e) -> state[0] == 3
                )
                .writeTo(Sinks.list("output"));

        var job = hz().getJet().newJob(p);
        job.join();

        IList<Object> output = member.getList("output");
        assertThat(output).containsExactly(2, 5);
    }

}

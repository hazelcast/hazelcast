/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class LightJobTest extends SimpleTestInClusterSupport {

    private static HazelcastInstance liteMember;

    @Parameter
    public TestMode testMode;

    @Parameters(name = "testMode={0}")
    public static Object[] parameters() {
        return TestMode.values();
    }

    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(2, null, null);
        liteMember = factory().newHazelcastInstance(smallInstanceConfig().setLiteMember(true));
    }

    private HazelcastInstance submittingInstance() {
        switch (testMode) {
            case CLIENT:
                return client();
            case MEMBER:
                return instance();
            case LITE_MEMBER:
                return liteMember;
        }
        throw new AssertionError("unexpected testMode=" + testMode);
    }

    @Test
    public void smokeTest_dag() {
        List<Integer> items = IntStream.range(0, 1_000).boxed().collect(Collectors.toList());

        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", processorFromPipelineSource(TestSources.items(items)));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(between(src, sink).distributed());

        submittingInstance().getJet().newLightJob(dag).join();
        List<Integer> result = instance().getList("sink");
        assertThat(result).containsExactlyInAnyOrderElementsOf(items);
    }

    @Test
    public void smokeTest_pipeline() {
        List<Integer> items = IntStream.range(0, 1_000).boxed().collect(Collectors.toList());

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(items))
                .writeTo(Sinks.list("sink"));

        submittingInstance().getJet().newLightJob(p).join();
        List<Integer> result = instance().getList("sink");
        assertThat(result).containsExactlyInAnyOrderElementsOf(items);
    }

    enum TestMode {
        CLIENT,
        MEMBER,
        LITE_MEMBER
    }
}

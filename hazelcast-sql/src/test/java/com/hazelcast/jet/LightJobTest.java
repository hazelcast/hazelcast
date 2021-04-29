/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.PacketFiltersUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class LightJobTest extends SimpleTestInClusterSupport {

    @Parameter
    public boolean useClient;

    @Parameters(name = "useClient={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(2, null, null);
    }

    private JetInstance submittingInstance() {
        return useClient ? client() : instance();
    }

    @Test
    public void test() {
        List<Integer> items = IntStream.range(0, 1_000).boxed().collect(Collectors.toList());

        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", processorFromPipelineSource(TestSources.items(items)));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(between(src, sink).distributed());

        submittingInstance().newLightJob(dag).join();
        List<Integer> result = instance().getList("sink");
        assertThat(result).containsExactlyInAnyOrderElementsOf(items);
    }

    @Test
    public void test_cancellation() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new MockP().streaming());

        LightJob job = submittingInstance().newLightJob(dag);
        // sleep a little to make it quite likely that the job is deployed to both members
        sleepMillis(100);
        job.cancel();
        assertThatThrownBy(job::join)
                .isInstanceOf(CancellationException.class);
    }

    @Test
    public void when_terminateOpLost_then_jobTerminatesAnyway() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new MockP().streaming());
        LightJob job = submittingInstance().newLightJob(dag);
        sleepSeconds(1);

        // When
        PacketFiltersUtil.dropOperationsFrom(instance().getHazelcastInstance(), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.TERMINATE_JOB_OP));
        job.cancel();

        // Then
        assertThatThrownBy(job::join)
                .isInstanceOf(CancellationException.class);
    }

    @Test
    public void test_cancelAfterCompleted() {
        DAG dag = new DAG();
        dag.newVertex("v", MockP::new);
        LightJob job = submittingInstance().newLightJob(dag);
        job.join();
        job.cancel();
        // join after late cancel won't fail with CancellationException because the job completed
        // before the cancellation
        job.join();
    }
}

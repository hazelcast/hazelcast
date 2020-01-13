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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestProcessors.ListSource;
import static com.hazelcast.jet.core.TestProcessors.MockP;
import static com.hazelcast.jet.core.TestProcessors.MockPMS;
import static com.hazelcast.jet.core.TestProcessors.MockPS;
import static com.hazelcast.jet.core.TestProcessors.reset;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JobLifecycleMetricsTest extends JetTestSupport {

    private static final int MEMBER_COUNT = 1;
    //TODO: need to test on multiple members, some metrics get set only on master

    private static final String PREFIX = "com.hazelcast.jet";

    private ObjectName objectNameWithModule;

    private MBeanServer platformMBeanServer;

    private JetInstance jetInstance;

    @Before
    public void before() throws Exception {
        reset(MEMBER_COUNT);

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMetricsConfig().setCollectionFrequencySeconds(1);

        jetInstance = createJetMember(config);

        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        objectNameWithModule = new ObjectName(PREFIX + ":*");
    }

    @After
    public void after() {
        Jet.shutdownAll();
    }

    @Test
    public void multipleJobsSubmittedAndCompleted() {
        //when
        Job job1 = jetInstance.newJob(batchPipeline());
        job1.join();
        job1.cancel();

        //then
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 1, 0));

        //given
        DAG dag = new DAG();
        Throwable e = new AssertionError("mock error");
        Vertex source = dag.newVertex("source", ListSource.supplier(singletonList(1)));
        Vertex process = dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setProcessError(e), MEMBER_COUNT)));
        dag.edge(between(source, process));

        //when
        Job job2 = jetInstance.newJob(dag);
        try {
            job2.join();
            fail("Expected exception not thrown!");
        } catch (Exception ex) {
            //ignore
        }

        //then
        assertTrueEventually(() -> assertJobStats(2, 2, 2, 1, 1));
    }

    @Test
    public void jobSuspendedThenResumed() {
        //init
        Job job = jetInstance.newJob(streamingPipeline());
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));

        //when
        job.suspend();
        assertTrueEventually(() -> assertEquals(SUSPENDED, job.getStatus()));

        //then
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 0));

        //when
        job.resume();
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));

        //then
        assertTrueEventually(() -> assertJobStats(1, 2, 1, 0, 0));
    }

    @Test
    public void jobRestarted() {
        //init
        Job job = jetInstance.newJob(streamingPipeline());
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));

        //when
        job.restart();
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));

        //then
        assertTrueEventually(() -> assertJobStats(1, 2, 1, 0, 0));
    }

    @Test
    public void jobCancelled() {
        //init
        Job job = jetInstance.newJob(streamingPipeline());
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));

        //when
        job.cancel();

        //then
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 1));
    }

    @Test
    public void executionRelatedMetrics() {
        Job job1 = jetInstance.newJob(batchPipeline(), new JobConfig().setStoreMetricsAfterJobCompletion(true));
        job1.join();
        JobMetrics metrics = job1.getMetrics();

        System.out.println(metrics);
        List<Measurement> startTime = metrics.get(MetricNames.EXECUTION_START_TIME);
        assertEquals(MEMBER_COUNT, startTime.size());
        long startTimeVal = startTime.get(0).value();
        assertTrue("startTime.value=" + startTimeVal, startTimeVal != 0);

        List<Measurement> completionTime = metrics.get(MetricNames.EXECUTION_COMPLETION_TIME);
        assertEquals(MEMBER_COUNT, completionTime.size());
        long completionTimeVal = completionTime.get(0).value();
        assertTrue("completionTime.value=" + completionTimeVal, completionTimeVal != 0);

        assertTrue("startTime=" + startTimeVal + ", completionTime=" + completionTimeVal,
                startTimeVal <= completionTimeVal);
    }

    private Pipeline batchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
                .writeTo(Sinks.logger());
        return p;
    }

    private Pipeline streamingPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(3))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return p;
    }

    private void assertJobStats(int submitted, int executionsStarted, int executionsTerminated,
                                int completedSuccessfully, int completedWithFailure) throws Exception {
        assertMBeans(
                PREFIX + ":type=Metrics,instance=" + jetInstance.getName(),
                Arrays.asList(
                        entry(MetricNames.JOBS_SUBMITTED, submitted),
                        entry(MetricNames.JOB_EXECUTIONS_STARTED, executionsStarted),
                        entry(MetricNames.JOB_EXECUTIONS_COMPLETED, executionsTerminated),
                        entry(MetricNames.JOBS_COMPLETED_SUCCESSFULLY, completedSuccessfully),
                        entry(MetricNames.JOBS_COMPLETED_WITH_FAILURE, completedWithFailure))
        );
    }

    private void assertMBeans(String name, List<Map.Entry<String, Number>> attributes) throws Exception {
        Set<ObjectInstance> instances = platformMBeanServer.queryMBeans(objectNameWithModule, null);

        ObjectName on = new ObjectName(name);

        Map<ObjectName, ObjectInstance> instanceMap =
                instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));
        assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));

        for (Map.Entry<String, Number> expectedAttribute : attributes) {
            MBeanInfo mBeanInfo = platformMBeanServer.getMBeanInfo(on);
            String key = expectedAttribute.getKey();
            long actualAttribute = (long) platformMBeanServer.getAttribute(on, key);
            assertEquals("Attribute '" + key + "' of '" + on + "' doesn't match",
                    expectedAttribute.getValue().longValue(), actualAttribute);
        }
    }

}

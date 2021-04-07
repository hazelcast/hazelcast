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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.config.Config;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.metrics.JobMetrics_BatchTest.JOB_CONFIG_WITH_METRICS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for JobMetrics that don't use shared cluster. The cluster in each test
 * has a specific configuration.
 */
public class JobMetrics_NonSharedClusterTest extends JetTestSupport {

    @Before
    public void before() {
        TestProcessors.reset(1);
    }

    @Test
    public void when_metricsCollectionOff_then_emptyMetrics() {
        Config config = new Config();
        config.getMetricsConfig().setEnabled(false);
        JetInstance inst = createJetMember(config);

        DAG dag = new DAG();
        dag.newVertex("v1", (SupplierEx<Processor>) NoOutputSourceP::new).localParallelism(1);
        Job job = inst.newJob(dag, JOB_CONFIG_WITH_METRICS);
        assertTrue(job.getMetrics().metrics().isEmpty());
    }

    @Test
    public void when_noMetricCollectionYet_then_emptyMetrics() {
        Config config = new Config();
        config.getMetricsConfig().setCollectionFrequencySeconds(10_000);
        JetInstance inst = createJetMember(config);

        DAG dag = new DAG();
        dag.newVertex("v1", (SupplierEx<Processor>) NoOutputSourceP::new).localParallelism(1);

        // Initial collection interval is 1 second. So let's run a job and wait until it has metrics.
        Job job1 = inst.newJob(dag, JOB_CONFIG_WITH_METRICS);
        try {
            JetTestSupport.assertTrueEventually(() -> assertFalse(job1.getMetrics().metrics().isEmpty()), 10);
        } catch (AssertionError e) {
            // If we don't get metrics in 10 seconds, ignore it, we probably missed the first collection
            // with this job. We might have caught a different error, let's log it at least.
            logger.warning("Ignoring this error: " + e, e);
        }

        // Let's do a second job for which we know there will be no metrics collection. It should
        // return empty metrics because the next collection will be in 10_000 seconds.
        Job job2 = inst.newJob(dag, JOB_CONFIG_WITH_METRICS);
        assertJobStatusEventually(job2, RUNNING);
        assertTrue(job2.getMetrics().metrics().isEmpty());
    }

}

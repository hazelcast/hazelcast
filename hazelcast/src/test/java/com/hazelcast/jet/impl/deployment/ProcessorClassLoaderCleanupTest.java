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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProcessorClassLoaderCleanupTest extends JetTestSupport {

    private JetService jet;
    private HazelcastInstance member;

    @Before
    public void setUp() throws Exception {
        member = createHazelcastInstance();
        jet = member.getJet();
    }

    @Test
    public void processorClassLoaderRemovedAfterJobFinished() throws Exception {
        Pipeline p = Pipeline.create();

        BatchSource<Integer> source = TestSources.items(1, 2, 3);
        p.readFrom(source).setLocalParallelism(1)
         .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        File file = new File("target/classes");
        jobConfig.addCustomClasspath(source.name(), file.toURI().toURL().toString());
        Job job = jet.newJob(p, jobConfig);
        job.join();

        JetServiceBackend jetServiceBackend =
                ((HazelcastInstanceProxy) member).getOriginal().node.getNodeEngine().getService(JetServiceBackend.SERVICE_NAME);
        JobExecutionService jobExecutionService = jetServiceBackend.getJobExecutionService();

        assertThatThrownBy(() -> jobExecutionService.getProcessorClassLoader(job.getId(), source.name()))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Processor classloader for jobId=" + job.getId()
                        + " requested, but it does not exists");
    }
}

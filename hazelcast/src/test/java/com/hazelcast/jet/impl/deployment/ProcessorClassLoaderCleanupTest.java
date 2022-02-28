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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobClassLoaderService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.JarUtil;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProcessorClassLoaderCleanupTest extends JetTestSupport {

    private static File jarFile;

    private JetService jet;
    private HazelcastInstance member;

    @BeforeClass
    public static void beforeClass() throws Exception {
        jarFile = File.createTempFile("resources_", ".jar");
        JarUtil.createResourcesJarFile(jarFile);

        System.setProperty(ClusterProperty.PROCESSOR_CUSTOM_LIB_DIR.getName(), System.getProperty("java.io.tmpdir"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (jarFile != null) {
            jarFile.delete();
            jarFile = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        member = createHazelcastInstance();
        jet = member.getJet();
    }

    @Test
    public void processorClassLoaderRemovedAfterJobFinished() throws Exception {
        Pipeline p = Pipeline.create();

        StreamSource<SimpleEvent> source = TestSources.itemStream(1);
        p.readFrom(source).withoutTimestamps()
                .setLocalParallelism(1)
         .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        jobConfig.addCustomClasspath(source.name(), jarFile.getName());
        Job job = jet.newJob(p, jobConfig);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        JetServiceBackend jetServiceBackend =
                ((HazelcastInstanceProxy) member).getOriginal().node.getNodeEngine().getService(JetServiceBackend.SERVICE_NAME);
        JobClassLoaderService jobClassLoaderService = jetServiceBackend.getJobClassLoaderService();

        ChildFirstClassLoader classLoader = (ChildFirstClassLoader) jobClassLoaderService.getProcessorClassLoader(job.getId(), source.name());

        job.suspend();
        assertJobStatusEventually(job, JobStatus.SUSPENDED);

        assertThat(classLoader.isClosed())
                .describedAs("classloader hasn't been closed")
                .isTrue();

        assertThatThrownBy(() -> jobClassLoaderService.getProcessorClassLoader(job.getId(), source.name()))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("JobClassLoaders for jobId=" + Util.idToString(job.getId())
                        + " requested, but it does not exists");
    }
}

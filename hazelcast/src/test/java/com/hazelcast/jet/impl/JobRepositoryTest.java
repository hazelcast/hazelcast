/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static com.hazelcast.spi.properties.ClusterProperty.JOB_RESULTS_MAX_SIZE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobRepositoryTest extends JetTestSupport {

    private static final long RESOURCES_EXPIRATION_TIME_MILLIS = SECONDS.toMillis(1);
    private static final int MAX_JOB_RESULTS_COUNT = 2;

    private final JobConfig jobConfig = new JobConfig();
    private HazelcastInstance instance;
    private JobRepository jobRepository;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(JOB_RESULTS_MAX_SIZE.getName(), Integer.toString(MAX_JOB_RESULTS_COUNT));
        config.getJetConfig().setEnabled(true).setResourceUploadEnabled(true);

        instance = createHazelcastInstance(config);
        jobRepository = new JobRepository(instance);
        jobRepository.setResourcesExpirationMillis(RESOURCES_EXPIRATION_TIME_MILLIS);

        TestProcessors.reset(2);
    }

    @Test
    public void when_jobIsComplete_then_resourcesImmediatelyDeleted() {
        jobConfig.addClass(DummyClass.class);
        var job = instance.getJet().newJob(newBatchPipeline(), jobConfig);
        job.join();
        assertThat((Map<?, ?>) jobRepository.getJobResources(job.getId())).isEmpty();
    }

    @Test
    public void when_jobIsCancelled_then_resourcesImmediatelyDeleted() {
        jobConfig.addClass(DummyClass.class);
        var job = instance.getJet().newJob(newStreamPipeline(), jobConfig);
        assertThat((Map<?, ?>) jobRepository.getJobResources(job.getId())).isNotEmpty();
        cancelAndJoin(job);
        assertThat((Map<?, ?>) jobRepository.getJobResources(job.getId())).isEmpty();
    }

    @Test
    public void when_jobIsRunning_then_expiredJobIsNotCleanedUp() {
        long jobId = uploadResourcesForNewJob();
        Data dag = createDagData();
        JobRecord jobRecord = createJobRecord(jobId, dag);
        jobRepository.putNewJobRecord(jobRecord);

        sleepUntilJobExpires();

        cleanup();

        assertNotNull(jobRepository.getJobRecord(jobId));
        assertFalse("job repository should not be empty", jobRepository.getJobResources(jobId).isEmpty());
    }

    @Test
    public void when_jobRecordIsPresentForExpiredJob_then_jobIsNotCleanedUp() {
        long jobId = uploadResourcesForNewJob();
        Data dag = createDagData();
        JobRecord jobRecord = createJobRecord(jobId, dag);
        jobRepository.putNewJobRecord(jobRecord);

        sleepUntilJobExpires();

        cleanup();

        assertNotNull(jobRepository.getJobRecord(jobId));
        assertFalse(jobRepository.getJobResources(jobId).isEmpty());
    }

    @Test
    public void when_onlyJobResourcesExist_then_jobResourcesClearedAfterExpiration() {
        // ensure that job records IMap exists - prerequisite for cleanup
        assertNotNull(instance.getMap(JobRepository.JOB_RECORDS_MAP_NAME));

        long jobId = uploadResourcesForNewJob();

        sleepUntilJobExpires();

        cleanup();

        assertTrue(jobRepository.getJobResources(jobId).isEmpty());
    }

    @Test
    public void when_jobJarUploadFails_then_jobResourcesCleanedUp() throws Exception {
        jobConfig.addJar(URI.create("http://site/nonexistent").toURL());
        testResourceCleanup();
    }

    @Test
    public void when_jobZipUploadFails_then_jobResourcesCleanedUp() throws Exception {
        jobConfig.addJarsInZip(URI.create("http://site/nonexistent").toURL());
        testResourceCleanup();
    }

    @Test
    public void when_jobClasspathResourceUploadFails_then_jobResourcesCleanedUp() throws Exception {
        jobConfig.addClasspathResource(URI.create("http://site/nonexistent").toURL());
        testResourceCleanup();
    }

    @Test
    public void when_jobFileUploadFails_then_jobResourcesCleanedUp() throws Exception {
        jobConfig.attachFile(URI.create("http://site/nonexistent").toURL());
        testResourceCleanup();
    }

    @Test
    public void when_jobSecondFileUploadFails_then_jobResourcesCleanedUp() throws Exception {
        File goodFile = File.createTempFile("job", "resource");
        try {
            jobConfig.attachFile(goodFile);
            jobConfig.attachFile(URI.create("http://site/nonexistent").toURL());
            testResourceCleanup();
        } finally {
            delete(goodFile);
        }
    }

    @Test
    public void when_jobDirectoryUploadFails_then_jobResourcesCleanedUp() throws Exception {
        // Given
        File baseDir = createTempDirectory();

        try {
            jobConfig.attachDirectory(baseDir);
        } finally { // Ensure dir deleted even if attachDirectory fails
            // When
            delete(baseDir);
        }
        // Then
        testResourceCleanup();
    }

    @Test
    public void when_jobSecondDirectoryUploadFails_then_jobResourcesCleanedUp() throws Exception {
        // Given
        File goodBaseDir = createTempDirectory();
        File baseDir = createTempDirectory();

        try {
            jobConfig.attachDirectory(goodBaseDir);
            jobConfig.attachDirectory(baseDir);
        } finally { // Ensure dir deleted even if attachDirectory fails
            // When
            delete(baseDir);
        }
        // Then
        testResourceCleanup();
    }

    private void testResourceCleanup() {
        try {
            jobRepository.uploadJobResources(jobRepository.newJobId(), jobConfig);
            fail();
        } catch (JetException e) {
            Collection<DistributedObject> objects = instance.getDistributedObjects();
            assertThat(objects).noneMatch(o -> o.getName().startsWith(JobRepository.RESOURCES_MAP_NAME_PREFIX));
        }
    }

    private void delete(File file) {
        assertTrue("Couldn't delete " + file, file.delete());
    }

    @Test
    public void test_getJobRecordFromClient() {
        HazelcastInstance client = createHazelcastClient();
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.streamFromProcessor("source", ProcessorMetaSupplier.of(() -> new NoOutputSourceP())))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        Job job = instance.getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(100));
        JobRepository jobRepository = new JobRepository(client);
        assertTrueEventually(() -> assertNotNull(jobRepository.getJobRecord(job.getId())));
        client.shutdown();
    }

    @Test
    public void test_getJobExecutionRecordFromClient() {
        HazelcastInstance client = createHazelcastClient();
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.streamFromProcessor("source", ProcessorMetaSupplier.of(() -> new NoOutputSourceP())))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        Job job = instance.getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(100));
        JobRepository jobRepository = new JobRepository(client);
        // This simulates operation executed by Management Center (see JobManager.getSnapshotDetails)
        assertTrueEventually(() -> assertNotNull(jobRepository.getJobExecutionRecord(job.getId())));
        client.shutdown();
    }

    @Test
    public void test_maxNumberOfJobResults() {
        DAG dag = new DAG();
        dag.newVertex("v", Processors.noopP());

        // create max+1 jobs
        for (int i = 0; i < MAX_JOB_RESULTS_COUNT + 1; i++) {
            instance.getJet().newJob(dag).join();
        }

        jobRepository.cleanup(Accessors.getNodeEngineImpl(instance));
        assertTrueEventually(() -> assertEquals(MAX_JOB_RESULTS_COUNT, jobRepository.getJobResults().size()));
    }

    private void cleanup() {
        jobRepository.cleanup(Accessors.getNodeEngineImpl(instance));
    }

    private long uploadResourcesForNewJob() {
        jobConfig.addClass(DummyClass.class);
        long jobId = jobRepository.newJobId();
        jobRepository.uploadJobResources(jobId, jobConfig);
        return jobId;
    }

    private Data createDagData() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new TestProcessors.MockP().streaming());
        return Accessors.getNodeEngineImpl(instance).toData(dag);
    }

    private JobRecord createJobRecord(long jobId, Data dag) {
        return new JobRecord(instance.getCluster().getLocalMember().getVersion().asVersion(),
                jobId, dag, "", jobConfig, Collections.emptySet(), null);
    }

    private void sleepUntilJobExpires() {
        sleepAtLeastMillis(2 * RESOURCES_EXPIRATION_TIME_MILLIS);
    }

    private static class DummyClass {
    }

    private static Pipeline newBatchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1)).writeTo(Sinks.noop());
        return p;
    }

    private static Pipeline newStreamPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(1)).withoutTimestamps().writeTo(Sinks.noop());
        return p;
    }
}

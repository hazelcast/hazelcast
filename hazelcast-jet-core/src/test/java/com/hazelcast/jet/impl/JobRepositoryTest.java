/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.jet.impl.JobRepository.RANDOM_IDS_MAP_NAME;
import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_SCAN_PERIOD;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JobRepositoryTest extends JetTestSupport {

    private static final long JOB_EXPIRATION_TIME_IN_MILLIS = SECONDS.toMillis(1);
    private static final long JOB_SCAN_PERIOD_IN_MILLIS = HOURS.toMillis(1);
    private static final int QUORUM_SIZE = 2;

    private JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private JobConfig jobConfig = new JobConfig();
    private JetInstance instance;
    private JobRepository jobRepository;
    private IMap<Long, Long> jobIds;

    @Before
    public void setup() {
        JetConfig config = new JetConfig();
        Properties properties = config.getProperties();
        properties.setProperty(JOB_SCAN_PERIOD.getName(), Long.toString(JOB_SCAN_PERIOD_IN_MILLIS));

        instance = factory.newMember(config);
        jobRepository = new JobRepository(instance, null);
        jobRepository.setJobExpirationDurationInMillis(JOB_EXPIRATION_TIME_IN_MILLIS);

        jobIds = instance.getMap(RANDOM_IDS_MAP_NAME);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void when_jobIsCompleted_then_jobIsCleanedUp() {
        long jobIb = uploadResourcesForNewJob();
        Data dag = createDAGData();
        JobRecord jobRecord = createJobRecord(jobIb, dag);
        jobRepository.putNewJobRecord(jobRecord);
        long executionId1 = jobRepository.newExecutionId(jobIb);
        long executionId2 = jobRepository.newExecutionId(jobIb);

        jobRepository.cleanup(singleton(jobIb), emptySet());

        assertNull(jobRepository.getJob(jobIb));
        assertTrue(jobRepository.getJobResources(jobIb).isEmpty());
        assertFalse(jobIds.containsKey(executionId1));
        assertFalse(jobIds.containsKey(executionId2));
    }

    @Test
    public void when_jobIsRunning_then_expiredJobIsNotCleanedUp() {
        long jobIb = uploadResourcesForNewJob();
        Data dag = createDAGData();
        JobRecord jobRecord = createJobRecord(jobIb, dag);
        jobRepository.putNewJobRecord(jobRecord);
        long executionId1 = jobRepository.newExecutionId(jobIb);
        long executionId2 = jobRepository.newExecutionId(jobIb);

        sleepUntilJobExpires();

        jobRepository.cleanup(emptySet(), singleton(jobIb));

        assertNotNull(jobRepository.getJob(jobIb));
        assertFalse(jobRepository.getJobResources(jobIb).isEmpty());
        assertTrue(jobIds.containsKey(executionId1));
        assertTrue(jobIds.containsKey(executionId2));
    }

    @Test
    public void when_jobRecordIsPresentForExpiredJob_then_jobIsNotCleanedUp() {
        long jobIb = uploadResourcesForNewJob();
        Data dag = createDAGData();
        JobRecord jobRecord = createJobRecord(jobIb, dag);
        jobRepository.putNewJobRecord(jobRecord);
        long executionId1 = jobRepository.newExecutionId(jobIb);
        long executionId2 = jobRepository.newExecutionId(jobIb);

        sleepUntilJobExpires();

        jobRepository.cleanup(emptySet(), emptySet());

        assertNotNull(jobRepository.getJob(jobIb));
        assertFalse(jobRepository.getJobResources(jobIb).isEmpty());
        assertTrue(jobIds.containsKey(executionId1));
        assertTrue(jobIds.containsKey(executionId2));
    }

    @Test
    public void when_onlyJobResourcesExist_then_jobResourcesClearedAfterExpiration() {
        long jobIb = uploadResourcesForNewJob();

        sleepUntilJobExpires();

        jobRepository.cleanup(emptySet(), emptySet());

        assertTrue(jobRepository.getJobResources(jobIb).isEmpty());
    }

    @Test
    public void when_jobResourceUploadFails_then_jobResourcesCleanedUp() {
        jobConfig.addResource("invalid path");
        try {
            jobRepository.uploadJobResources(jobConfig);
            fail();
        } catch (JetException e) {
            assertTrue(instance.getMap(RANDOM_IDS_MAP_NAME).isEmpty());
        }
    }

    @Test
    public void when_newQuorumSizeIsLargerThanCurrent_then_jobQuorumSizeIsUpdated() {
        long jobIb = uploadResourcesForNewJob();
        Data dag = createDAGData();
        JobRecord jobRecord = createJobRecord(jobIb, dag);
        jobRepository.putNewJobRecord(jobRecord);

        int newQuorumSize = jobRecord.getQuorumSize() + 1;
        boolean success = jobRepository.updateJobQuorumSizeIfLargerThanCurrent(jobIb, newQuorumSize);

        assertTrue(success);
        jobRecord = jobRepository.getJob(jobIb);
        assertEquals(newQuorumSize, jobRecord.getQuorumSize());
    }

    @Test
    public void when_newQuorumSizeIsNotLargerThanCurrent_then_jobQuorumSizeIsNotUpdated() {
        long jobIb = uploadResourcesForNewJob();
        Data dag = createDAGData();
        JobRecord jobRecord = createJobRecord(jobIb, dag);
        jobRepository.putNewJobRecord(jobRecord);

        int currentQuorumSize = jobRecord.getQuorumSize();
        int newQuorumSize = currentQuorumSize - 1;
        boolean success = jobRepository.updateJobQuorumSizeIfLargerThanCurrent(jobIb, newQuorumSize);

        assertFalse(success);
        jobRecord = jobRepository.getJob(jobIb);
        assertEquals(currentQuorumSize, jobRecord.getQuorumSize());
    }

    @Test
    public void when_jobIsMissing_then_jobQuorumSizeIsNotUpdated() {
        long jobIb = uploadResourcesForNewJob();

        boolean success = jobRepository.updateJobQuorumSizeIfLargerThanCurrent(jobIb, 1);

        assertFalse(success);
        assertNull(jobRepository.getJob(jobIb));
    }

    private long uploadResourcesForNewJob() {
        jobConfig.addClass(DummyClass.class);
        return jobRepository.uploadJobResources(jobConfig);
    }

    private Data createDAGData() {
        return getNodeEngineImpl(instance.getHazelcastInstance()).toData(new DAG());
    }

    private JobRecord createJobRecord(long jobIb, Data dag) {
        return new JobRecord(jobIb, System.currentTimeMillis(), dag, jobConfig, QUORUM_SIZE);
    }

    private void sleepUntilJobExpires() {
        sleepAtLeastMillis(2 * JOB_EXPIRATION_TIME_IN_MILLIS);
    }


    static class DummyClass {

    }

}

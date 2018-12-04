/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.core.Member;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Optional;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * This class tests Job operations when a master doesn't know of the job
 * because it restarted. All tests check that it fails as it should.
 */
@RunWith(HazelcastSerialClassRunner.class)
public class Job_StaleInstanceTest extends JetTestSupport {

    private static JetTestInstanceFactory instanceFactory;
    private static JetInstance client;
    private static Job job;

    @BeforeClass
    public static void beforeClass() {
        TestProcessors.reset(1);
        instanceFactory = new JetTestInstanceFactory();
        JetInstance instance = instanceFactory.newMember();
        DAG dag = new DAG();
        dag.newVertex("v", () -> new NoOutputSourceP());
        client = instanceFactory.newClient();
        job = client.newJob(dag);
        assertJobStatusEventually(job, RUNNING);

        instance.getHazelcastInstance().getLifecycleService().terminate();
        instance = instanceFactory.newMember();
        assertEqualsEventually(() -> firstItem(client.getHazelcastInstance().getCluster().getMembers())
                        .map(Member::getAddress).orElse(null),
                instance.getHazelcastInstance().getCluster().getLocalMember().getAddress());
    }

    private static <E> Optional<E> firstItem(Iterable<E> iterable) {
        Iterator<E> iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            return Optional.empty();
        }
        return Optional.of(iterator.next());
    }

    @AfterClass
    public static void afterClass() {
        TestProcessors.reset(1);
        instanceFactory.shutdownAll();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_restart() {
        job.restart();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_suspend() {
        job.suspend();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_resume() {
        job.resume();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_cancel() {
        job.cancel();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_getStatus() {
        job.getStatus();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_getFuture() {
        try {
            job.getFuture().get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Test(expected = JobNotFoundException.class)
    public void when_submissionTime() {
        job.getSubmissionTime();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_join() {
        try {
            job.join();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}

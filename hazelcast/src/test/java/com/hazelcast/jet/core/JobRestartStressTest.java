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

package com.hazelcast.jet.core;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class JobRestartStressTest extends JobRestartStressTestBase {
    @Test
    public void stressTest_restart() throws Exception {
        stressTest(tuple -> {
            logger.info("restarting");
            tuple.f2().restart();
            logger.info("restarted");
            // Sleep a little in order to not observe the RUNNING status from the execution
            // before the restart.
            LockSupport.parkNanos(MILLISECONDS.toNanos(500));
            assertJobStatusEventually(tuple.f2(), JobStatus.RUNNING);
            return tuple.f2();
        });
    }

    @Test
    public void stressTest_suspendAndResume() throws Exception {
        stressTest(tuple -> {
            logger.info("Suspending the job...");
            tuple.f2().suspend();
            logger.info("suspend() returned");
            assertJobStatusEventually(tuple.f2(), JobStatus.SUSPENDED, 15);
            // The Job.resume() call might overtake the suspension.
            // resume() does nothing when job is not suspended. Without
            // the sleep, the job might remain suspended.
            sleepSeconds(1);
            logger.info("Resuming the job...");
            tuple.f2().resume();
            logger.info("resume() returned");
            assertJobStatusEventually(tuple.f2(), JobStatus.RUNNING, 15);
            return tuple.f2();
        });
    }
}

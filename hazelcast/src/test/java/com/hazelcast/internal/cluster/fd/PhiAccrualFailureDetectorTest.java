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

package com.hazelcast.internal.cluster.fd;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhiAccrualFailureDetectorTest {

    private final double phiThreshold = 1;
    private final long minStdDev = 100;
    private final long acceptableHeartbeatPause = 1000;

    private FailureDetector failureDetector = new PhiAccrualFailureDetector(phiThreshold, 100, minStdDev,
            acceptableHeartbeatPause, minStdDev);

    @Test
    public void member_isAssumedAlive_beforeFirstHeartbeat() {
        assertTrue(failureDetector.isAlive(Clock.currentTimeMillis()));
    }

    @Test
    public void member_isAlive_whenHeartbeat() {
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(timestamp);
        assertTrue(failureDetector.isAlive(timestamp));
    }

    @Test
    public void member_isAlive_beforeHeartbeatTimeout() {
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(timestamp);
        assertTrue(failureDetector.isAlive(timestamp + acceptableHeartbeatPause / 2));
    }

    @Test
    public void member_isNotAlive_afterHeartbeatTimeout() {
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(timestamp);

        long ts = timestamp + acceptableHeartbeatPause * 2;
        assertFalse("Suspicion level: " + failureDetector.suspicionLevel(ts), failureDetector.isAlive(ts));
    }

    @Test
    public void lastHeartbeat_whenNoHeartbeat() {
        long lastHeartbeat = failureDetector.lastHeartbeat();
        assertEquals(PhiAccrualFailureDetector.NO_HEARTBEAT_TIMESTAMP, lastHeartbeat);
    }

    @Test
    public void lastHeartbeat() {
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(timestamp);

        long lastHeartbeat = failureDetector.lastHeartbeat();
        assertEquals(timestamp, lastHeartbeat);
    }

    @Test
    public void nonSuspected_beforeFirstHeartbeat() {
        double suspicionLevel = failureDetector.suspicionLevel(Clock.currentTimeMillis());

        assertEquals(0, suspicionLevel, 0d);
    }

    @Test
    public void suspicionLevel_whenHeartbeat() {
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(timestamp);

        double suspicionLevel = failureDetector.suspicionLevel(timestamp);
        assertEquals(0, suspicionLevel, 0d);
    }

    @Test
    public void suspicionLevel_beforeHeartbeatTimeout() {
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(timestamp);

        double suspicionLevel = failureDetector.suspicionLevel(timestamp + acceptableHeartbeatPause / 2);

        assertThat(suspicionLevel, lessThan(phiThreshold));
    }

    @Test
    public void suspicionLevel_afterHeartbeatTimeout() {
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(timestamp);

        double suspicionLevel = failureDetector.suspicionLevel(timestamp + acceptableHeartbeatPause * 2);

        assertThat(suspicionLevel, greaterThanOrEqualTo(phiThreshold));
    }

    @Test(expected = IllegalArgumentException.class)
    public void construct_withNegativeThreshold() {
        new PhiAccrualFailureDetector(-1, 1, 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void construct_withZeroThreshold() {
        new PhiAccrualFailureDetector(0, 1, 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void construct_withNegativeMinStdDev() {
        new PhiAccrualFailureDetector(1, -1, 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void construct_withZeroMinStdDev() {
        new PhiAccrualFailureDetector(1, 0, 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void construct_withNegativeFirstHeartbeatEstimation() {
        new PhiAccrualFailureDetector(1, 1, 1, 1, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void construct_withZeroFirstHeartbeatEstimation() {
        new PhiAccrualFailureDetector(1, 1, 1, 1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void construct_withNegativeAcceptableHeartbeatPause() {
        new PhiAccrualFailureDetector(1, 1, 1, -1, 1);
    }
}

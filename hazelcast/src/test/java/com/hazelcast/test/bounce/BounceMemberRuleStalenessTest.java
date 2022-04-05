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

package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BounceMemberRuleStalenessTest extends HazelcastTestSupport {
    private static final int MAXIMUM_STALENESS_SECONDS = 5;

    // an intentionally long interval to test the failure is declared quickly
    // even when a bouncing interval is long
    private static final int BOUNCING_INTERVAL_SECONDS = 120;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(new Config())
            .clusterSize(2)
            .maximumStalenessSeconds(MAXIMUM_STALENESS_SECONDS)
            .bouncingIntervalSeconds(BOUNCING_INTERVAL_SECONDS)
            .driverType(BounceTestConfiguration.DriverType.MEMBER)
            .build();

    @Test
    public void stalenessIsDetected() {
        long startNanos = Timer.nanos();
        try {
            bounceMemberRule.testRepeatedly(1, new Runnable() {
                @Override
                public void run() {
                    sleepAtLeastMillis(10000);
                }
            }, 120);

            fail("The Bouncing Rule should detect a staleness!");
        } catch (AssertionError ae) {
            assertThat(ae).hasMessageStartingWith("Stalling task detected");
            long detectionDurationSeconds = Timer.secondsElapsed(startNanos);
            assertTrue("Staleness detector was too slow to detect stale. "
                            + "Maximum configured staleness in seconds: " + MAXIMUM_STALENESS_SECONDS
                            + " and it took " + detectionDurationSeconds + " seconds to detect staleness",
                    detectionDurationSeconds < BOUNCING_INTERVAL_SECONDS * 4);
        }
    }
}

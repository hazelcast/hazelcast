package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;



import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
            .build();

    @Test
    public void stalenessIsDetected() {
        long startTime = System.nanoTime();
        try {
            bounceMemberRule.testRepeatedly(1, new Runnable() {
                @Override
                public void run() {
                    sleepAtLeastMillis(10000);
                }
            }, 120);

            fail("The Bouncing Rule should detect a staleness!");
        } catch (AssertionError ae) {
            long detectionDurationSeconds = NANOSECONDS.toSeconds(System.nanoTime() - startTime);
            assertTrue("Staleness detector was too slow to detect stale. "
                    + "Maximum configured staleness in seconds: " + MAXIMUM_STALENESS_SECONDS
                    + " and it took " + detectionDurationSeconds + " seconds to detect staleness",
                    detectionDurationSeconds < BOUNCING_INTERVAL_SECONDS * 4);
        }
    }
}

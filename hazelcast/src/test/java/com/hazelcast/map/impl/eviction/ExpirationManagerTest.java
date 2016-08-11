/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS;
import static java.lang.String.valueOf;
import static java.lang.System.clearProperty;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExpirationManagerTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testTaskPeriodSeconds_set_viaSystemProperty() throws Exception {
        String previous = getProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS);
        try {
            int expectedPeriodSeconds = 12;
            setProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, valueOf(expectedPeriodSeconds));

            int actualTaskPeriodSeconds = new ExpirationManager(getMapServiceContext()).getTaskPeriodSeconds();

            assertEquals(expectedPeriodSeconds, actualTaskPeriodSeconds);
        } finally {
            restoreProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, previous);

        }
    }

    private void restoreProperty(String sysProp, String previous) {
        if (previous == null) {
            clearProperty(sysProp);
        } else {
            setProperty(sysProp, previous);
        }
    }

    @Test
    public void testTaskPeriodSeconds_throwsIllegalArgumentException_whenNotPositive() throws Exception {
        String previous = getProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS);
        try {
            setProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, valueOf(0));

            thrown.expectMessage("taskPeriodSeconds should be a positive number");
            thrown.expect(IllegalArgumentException.class);

            new ExpirationManager(getMapServiceContext());
        } finally {
            restoreProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, previous);
        }
    }

    @Test
    public void testCleanupPercentage_set_viaSystemProperty() throws Exception {
        String previous = getProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE);
        try {
            int expectedCleanupPercentage = 77;
            setProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, valueOf(expectedCleanupPercentage));

            int actualCleanupPercentage = new ExpirationManager(getMapServiceContext()).getCleanupPercentage();

            assertEquals(expectedCleanupPercentage, actualCleanupPercentage);
        } finally {
            restoreProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, previous);
        }
    }

    @Test
    public void testCleanupPercentage_throwsIllegalArgumentException_whenNotInRange() throws Exception {
        String previous = getProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE);
        try {
            setProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, valueOf(0));

            thrown.expectMessage("cleanupPercentage should be in range (0,100]");
            thrown.expect(IllegalArgumentException.class);

            new ExpirationManager(getMapServiceContext());
        } finally {
            restoreProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, previous);
        }
    }


    @Test
    public void testCleanupOperationCount_set_viaSystemProperty() throws Exception {
        String previous = getProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT);
        try {
            int expectedCleanupOperationCount = 19;
            setProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, valueOf(expectedCleanupOperationCount));

            int actualCleanupOperationCount = new ExpirationManager(getMapServiceContext()).getCleanupOperationCount();

            assertEquals(expectedCleanupOperationCount, actualCleanupOperationCount);
        } finally {
            restoreProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, previous);
        }
    }

    @Test
    public void testCleanupOperationCount_throwsIllegalArgumentException_whenNotPositive() throws Exception {
        String previous = getProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT);
        try {
            setProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, valueOf(0));

            thrown.expectMessage("cleanupOperationCount should be a positive number");
            thrown.expect(IllegalArgumentException.class);

            new ExpirationManager(getMapServiceContext());
        } finally {
            restoreProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, previous);
        }
    }

    private MapServiceContext getMapServiceContext() {
        HazelcastInstance node = createHazelcastInstance();
        return ((MapService) getNodeEngineImpl(node).getService(SERVICE_NAME)).getMapServiceContext();
    }
}
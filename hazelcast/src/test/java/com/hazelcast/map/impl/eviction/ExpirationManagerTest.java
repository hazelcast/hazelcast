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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS;
import static java.lang.String.valueOf;
import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ExpirationManagerTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private MapServiceContext mapServiceContext;

    @Before
    public void setUp() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        mapServiceContext
                = ((MapService) getNodeEngineImpl(node).getService(MapService.SERVICE_NAME)).getMapServiceContext();
    }

    @Test
    public void testTaskPeriodSeconds_set_viaSystemProperty() throws Exception {
        int expectedPeriodSeconds = 12;
        setProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, valueOf(expectedPeriodSeconds));
        int actualTaskPeriodSeconds = new ExpirationManager(mapServiceContext).getTaskPeriodSeconds();
        clearProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS);

        assertEquals(expectedPeriodSeconds, actualTaskPeriodSeconds);
    }

    @Test
    public void testTaskPeriodSeconds_throwsIllegalArgumentException_whenNotPositive() throws Exception {
        try {
            setProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, valueOf(0));

            thrown.expectMessage("taskPeriodSeconds should be a positive number");
            thrown.expect(IllegalArgumentException.class);

            new ExpirationManager(mapServiceContext);
        } finally {
            clearProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS);
        }
    }

    @Test
    public void testCleanupPercentage_set_viaSystemProperty() throws Exception {
        int expectedCleanupPercentage = 77;
        setProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, valueOf(expectedCleanupPercentage));
        int actualCleanupPercentage = new ExpirationManager(mapServiceContext).getCleanupPercentage();
        clearProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE);

        assertEquals(expectedCleanupPercentage, actualCleanupPercentage);
    }

    @Test
    public void testCleanupPercentage_throwsIllegalArgumentException_whenNotInRange() throws Exception {
        try {
            setProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, valueOf(0));

            thrown.expectMessage("cleanupPercentage should be in range (0,100]");
            thrown.expect(IllegalArgumentException.class);

            new ExpirationManager(mapServiceContext);
        } finally {
            clearProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE);
        }
    }


    @Test
    public void testCleanupOperationCount_set_viaSystemProperty() throws Exception {
        int expectedCleanupOperationCount = 19;
        setProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, valueOf(expectedCleanupOperationCount));
        int actualCleanupOperationCount = new ExpirationManager(mapServiceContext).getCleanupOperationCount();
        clearProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT);

        assertEquals(expectedCleanupOperationCount, actualCleanupOperationCount);
    }

    @Test
    public void testCleanupOperationCount_throwsIllegalArgumentException_whenNotPositive() throws Exception {
        try {
            setProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, valueOf(0));

            thrown.expectMessage("cleanupOperationCount should be a positive number");
            thrown.expect(IllegalArgumentException.class);

            new ExpirationManager(mapServiceContext);
        } finally {
            clearProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT);
        }
    }
}
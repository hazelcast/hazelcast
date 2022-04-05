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

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.internal.cluster.Versions.PREVIOUS_CLUSTER_VERSION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractDistributedObjectTest extends HazelcastTestSupport {

    private static final Version NEXT_MINOR_VERSION =
            Version.of(CURRENT_CLUSTER_VERSION.getMajor(), CURRENT_CLUSTER_VERSION.getMinor() + 1);

    private AbstractDistributedObject object;

    @Before
    public void setup() {
        HazelcastInstance instance = createHazelcastInstance();
        object = (AbstractDistributedObject) instance.getMap("test");
    }

    @Test
    public void testClusterVersionIsNotUnknown() {
        assertFalse(object.isClusterVersionUnknown());
    }

    @Test
    public void testClusterVersion_isEqualTo_currentVersion() {
        assertTrue(object.isClusterVersionEqualTo(CURRENT_CLUSTER_VERSION));
    }

    @Test
    public void testClusterVersion_isGreaterOrEqual_currentVersion() {
        assertTrue(object.isClusterVersionGreaterOrEqual(CURRENT_CLUSTER_VERSION));
    }

    @Test
    public void testClusterVersion_isUnknownGreaterOrEqual_currentVersion() {
        assertTrue(object.isClusterVersionUnknownOrGreaterOrEqual(CURRENT_CLUSTER_VERSION));
    }

    @Test
    public void testClusterVersion_isGreaterThan_previousVersion() {
        assertTrue(object.isClusterVersionGreaterThan(PREVIOUS_CLUSTER_VERSION));
    }

    @Test
    public void testClusterVersion_isUnknownGreaterThan_previousVersion() {
        assertTrue(object.isClusterVersionUnknownOrGreaterThan(PREVIOUS_CLUSTER_VERSION));
    }

    @Test
    public void testClusterVersion_isLessOrEqual_currentVersion() {
        assertTrue(object.isClusterVersionLessOrEqual(CURRENT_CLUSTER_VERSION));
    }

    @Test
    public void testClusterVersion_isUnknownLessOrEqual_currentVersion() {
        assertTrue(object.isClusterVersionUnknownOrLessOrEqual(CURRENT_CLUSTER_VERSION));
    }

    @Test
    public void testClusterVersion_isLessThan_nextMinorVersion() {
        assertTrue(object.isClusterVersionLessThan(NEXT_MINOR_VERSION));
    }

    @Test
    public void testClusterVersion_isUnknownOrLessThan_nextMinorVersion() {
        assertTrue(object.isClusterVersionUnknownOrLessThan(NEXT_MINOR_VERSION));
    }
}

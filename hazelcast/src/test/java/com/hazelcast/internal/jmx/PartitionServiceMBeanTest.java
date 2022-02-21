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

package com.hazelcast.internal.jmx;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.monitor.impl.MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionServiceMBeanTest extends HazelcastTestSupport {

    public static final String TYPE_NAME = "HazelcastInstance.PartitionServiceMBean";

    private MBeanDataHolder holder;
    private String hzName;

    @Before
    public void setUp() throws Exception {
        holder = new MBeanDataHolder(createHazelcastInstanceFactory(1));
        hzName = holder.getHz().getName();
    }

    @Test
    public void testPartitionCount() throws Exception {
        assertEquals(DEFAULT_PARTITION_COUNT, (int) getIntAttribute("partitionCount"));
    }

    @Test
    public void testActivePartitionCount() throws Exception {
        warmUpPartitions(holder.getHz());
        assertEquals(DEFAULT_PARTITION_COUNT, (int) getIntAttribute("activePartitionCount"));
    }

    @Test
    public void testClusterSafe() throws Exception {
        assertTrue((Boolean) holder.getMBeanAttribute(TYPE_NAME, hzName, "isClusterSafe"));
    }

    @Test
    public void testLocalMemberSafe() throws Exception {
        assertTrue((Boolean) holder.getMBeanAttribute(TYPE_NAME, hzName, "isLocalMemberSafe"));
    }

    private Integer getIntAttribute(String name) throws Exception {
        return (Integer) holder.getMBeanAttribute(TYPE_NAME, hzName, name);
    }
}

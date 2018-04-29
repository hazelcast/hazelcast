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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapConfigReadOnlyTest {

    private ReplicatedMapConfig getReadOnlyConfig() {
        return new ReplicatedMapConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetReplicatorExecutorService() {
        getReadOnlyConfig().setReplicatorExecutorService(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetName() {
        getReadOnlyConfig().setName("anyName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetReplicationDelayMillis() {
        getReadOnlyConfig().setReplicationDelayMillis(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetConcurrencyLevel() {
        getReadOnlyConfig().setConcurrencyLevel(42);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetInMemoryFormat() {
        getReadOnlyConfig().setInMemoryFormat(InMemoryFormat.BINARY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetListenerConfigs() {
        getReadOnlyConfig().setListenerConfigs(Collections.<ListenerConfig>emptyList());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAsyncFillup() {
        getReadOnlyConfig().setAsyncFillup(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStatisticsEnabled() {
        getReadOnlyConfig().setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetQuorumName() {
        getReadOnlyConfig().setQuorumName("myQuorum");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetMergePolicy() {
        getReadOnlyConfig().setMergePolicy("MyMergePolicy");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetMergePolicyConfig() {
        getReadOnlyConfig().setMergePolicyConfig(new MergePolicyConfig());
    }

    @Test
    public void testGetReadOnly_returnsSameInstance() {
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig(randomName());
        ReplicatedMapConfig readOnly = replicatedMapConfig.getAsReadOnly();
        assertSame(readOnly, replicatedMapConfig.getAsReadOnly());
    }
}

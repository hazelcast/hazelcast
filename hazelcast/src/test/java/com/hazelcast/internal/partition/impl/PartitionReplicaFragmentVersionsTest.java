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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionReplicaFragmentVersionsTest extends TestCase {

    private PartitionReplicaFragmentVersions versions = new PartitionReplicaFragmentVersions(1,
            new DistributedObjectNamespace("svc", "obj"));

    @Test
    public void testIncrementAndGet() {
        versions.set(new long[] {-3, -2, -2, -2, -2, -2}, 1);
        long[] updated = versions.incrementAndGet(2);
        // -1 has special meaning (REQUIRES_SYNC) and is skipped while incrementing
        Assert.assertArrayEquals(Arrays.toString(versions.get()),
                new long[] {-2, 0, -2, -2, -2, -2}, updated);
    }

    @Test
    public void testSet() {
        long[] expected = new long[] {-5, -3, -2, 4, 1, 2};
        versions.set(expected, 1);
        Assert.assertArrayEquals(Arrays.toString(versions.get()), expected, versions.get());
    }

    @Test
    public void testSetFromReplica3() {
        long[] expected = new long[] {-5, -3, -2, 4, 1, 2};
        long[] expectedFromReplica3 = new long[] {0, 0, -2, 4, 1, 2};
        versions.set(expected, 3);
        Assert.assertArrayEquals(Arrays.toString(versions.get()), expectedFromReplica3, versions.get());
    }

}

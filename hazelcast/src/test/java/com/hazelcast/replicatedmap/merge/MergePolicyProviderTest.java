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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MergePolicyProviderTest extends HazelcastTestSupport {

    private MergePolicyProvider mergePolicyProvider;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void given() {
        mergePolicyProvider = new MergePolicyProvider(getNode(createHazelcastInstance()).getNodeEngine());
    }

    @Test
    public void getMergePolicy_NotExistingMergePolicy() {
        expected.expect(HazelcastException.class);
        expected.expectCause(IsInstanceOf.any(ClassNotFoundException.class));
        mergePolicyProvider.getMergePolicy("no such policy bro!");
    }

    @Test
    public void getMergePolicy_NullPolicy() {
        expected.expect(NullPointerException.class);
        mergePolicyProvider.getMergePolicy(null);
    }

    @Test
    public void getMergePolicy_PutIfAbsentMapMergePolicy() {
        assertMergePolicyCorrectlyInitialised("com.hazelcast.replicatedmap.merge.PutIfAbsentMapMergePolicy");
    }

    @Test
    public void getMergePolicy_LatestUpdateMapMergePolicy() {
        assertMergePolicyCorrectlyInitialised("com.hazelcast.replicatedmap.merge.LatestUpdateMapMergePolicy");
    }

    @Test
    public void getMergePolicy_PassThroughMergePolicy() {
        assertMergePolicyCorrectlyInitialised("com.hazelcast.replicatedmap.merge.PassThroughMergePolicy");
    }

    @Test
    public void getMergePolicy_HigherHitsMapMergePolicy() {
        assertMergePolicyCorrectlyInitialised("com.hazelcast.replicatedmap.merge.HigherHitsMapMergePolicy");
    }

    private void assertMergePolicyCorrectlyInitialised(String mergePolicyName) {
        Object mergePolicy = mergePolicyProvider.getMergePolicy(mergePolicyName);

        assertNotNull(mergePolicy);
        assertEquals(mergePolicyName, mergePolicy.getClass().getName());
    }
}

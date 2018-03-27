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

package com.hazelcast.spi.merge;

import com.hazelcast.config.InvalidConfigurationException;
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
public class SplitBrainMergePolicyProviderTest extends HazelcastTestSupport {

    private SplitBrainMergePolicyProvider mergePolicyProvider;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setup() {
        mergePolicyProvider = new SplitBrainMergePolicyProvider(getNode(createHazelcastInstance()).getNodeEngine());
    }

    @Test
    public void getMergePolicy_NotExistingMergePolicy() {
        expected.expect(InvalidConfigurationException.class);
        expected.expectCause(IsInstanceOf.any(ClassNotFoundException.class));
        mergePolicyProvider.getMergePolicy("No such policy!");
    }

    @Test
    public void getMergePolicy_NullPolicy() {
        expected.expect(InvalidConfigurationException.class);
        mergePolicyProvider.getMergePolicy(null);
    }

    @Test
    public void getMergePolicy_HigherHitsMapCachePolicy_byFullyQualifiedName() {
        assertMergePolicyCorrectlyInitialised(HigherHitsMergePolicy.class.getName(), HigherHitsMergePolicy.class);
    }

    @Test
    public void getMergePolicy_HigherHitsMapCachePolicy_bySimpleName() {
        assertMergePolicyCorrectlyInitialised(HigherHitsMergePolicy.class.getSimpleName(), HigherHitsMergePolicy.class);
    }

    @Test
    public void getMergePolicy_LatestAccessCacheMergePolicy_byFullyQualifiedName() {
        assertMergePolicyCorrectlyInitialised(LatestAccessMergePolicy.class.getName(), LatestAccessMergePolicy.class);
    }

    @Test
    public void getMergePolicy_LatestAccessCacheMergePolicy_bySimpleName() {
        assertMergePolicyCorrectlyInitialised(LatestAccessMergePolicy.class.getSimpleName(), LatestAccessMergePolicy.class);
    }

    @Test
    public void getMergePolicy_LatestUpdateMergePolicy_byFullyQualifiedName() {
        assertMergePolicyCorrectlyInitialised(LatestUpdateMergePolicy.class.getName(), LatestUpdateMergePolicy.class);
    }

    @Test
    public void getMergePolicy_LatestUpdateMergePolicy_bySimpleName() {
        assertMergePolicyCorrectlyInitialised(LatestUpdateMergePolicy.class.getSimpleName(), LatestUpdateMergePolicy.class);
    }

    @Test
    public void getMergePolicy_PassThroughCachePolicy_byFullyQualifiedName() {
        assertMergePolicyCorrectlyInitialised(PassThroughMergePolicy.class.getName(), PassThroughMergePolicy.class);
    }

    @Test
    public void getMergePolicy_PassThroughCachePolicy_bySimpleName() {
        assertMergePolicyCorrectlyInitialised(PassThroughMergePolicy.class.getSimpleName(), PassThroughMergePolicy.class);
    }

    @Test
    public void getMergePolicy_PutIfAbsentCacheMergePolicy_byFullyQualifiedName() {
        assertMergePolicyCorrectlyInitialised(PutIfAbsentMergePolicy.class.getName(), PutIfAbsentMergePolicy.class);
    }

    @Test
    public void getMergePolicy_PutIfAbsentCacheMergePolicy_bySimpleName() {
        assertMergePolicyCorrectlyInitialised(PutIfAbsentMergePolicy.class.getSimpleName(), PutIfAbsentMergePolicy.class);
    }

    private void assertMergePolicyCorrectlyInitialised(String mergePolicyName,
                                                       Class<? extends SplitBrainMergePolicy> expectedMergePolicyClass) {
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(mergePolicyName);

        assertNotNull(mergePolicy);
        assertEquals(expectedMergePolicyClass, mergePolicy.getClass());
    }
}

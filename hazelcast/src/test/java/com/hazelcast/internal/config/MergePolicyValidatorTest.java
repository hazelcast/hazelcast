/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMergePolicySupportsInMemoryFormat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MergePolicyValidatorTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(MergePolicyValidatorTest.class);

    private SplitBrainMergePolicyProvider mapMergePolicyProvider;

    @Before
    public void setUp() {
        Config config = new Config();
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        when(nodeEngine.getConfigClassLoader()).thenReturn(config.getClassLoader());

        mapMergePolicyProvider = new SplitBrainMergePolicyProvider(nodeEngine);
        when(nodeEngine.getSplitBrainMergePolicyProvider()).thenReturn(mapMergePolicyProvider);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(MergePolicyValidator.class);
    }

    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withMergePolicy_OBJECT() {
        Object mergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertTrue(checkMergePolicySupportsInMemoryFormat("myMap", mergePolicy, OBJECT, false, LOGGER));
    }

    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withMergePolicy_NATIVE() {
        Object mergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertTrue(checkMergePolicySupportsInMemoryFormat("myMap", mergePolicy, NATIVE, false, LOGGER));
    }

}

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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import com.hazelcast.test.starter.constructor.MergePolicyProviderConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.starter.HazelcastStarterUtils.assertInstanceOfByClassName;
import static com.hazelcast.test.starter.ReflectionUtils.getDelegateFromMock;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MergePolicyProviderConstructorTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void setUp() {
        Config config = new Config();
        hz = HazelcastStarter.newHazelcastInstance("3.10", config, false);
    }

    @After
    public void tearDown() {
        hz.shutdown();
    }

    @Test
    public void testConstructor() throws Exception {
        //this is testing the Hazelcast Started can create an instances of merge policies

        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        CacheService service = nodeEngine.getService(CacheService.SERVICE_NAME);
        CacheMergePolicyProvider mergePolicyProvider = service.getMergePolicyProvider();

        Object delegate = getDelegateFromMock(service);
        Object mergePolicyProviderProxy = getFieldValueReflectively(delegate, "mergePolicyProvider");

        MergePolicyProviderConstructor constructor = new MergePolicyProviderConstructor(CacheMergePolicyProvider.class);
        CacheMergePolicyProvider clonedMergePolicyProvider
                = (CacheMergePolicyProvider) constructor.createNew(mergePolicyProviderProxy);

        // invalid merge policy
        assertInvalidCacheMergePolicy(mergePolicyProvider);
        assertInvalidCacheMergePolicy(clonedMergePolicyProvider);
        // legacy merge policy
        assertCacheMergePolicy(mergePolicyProvider, "com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy");
        assertCacheMergePolicy(clonedMergePolicyProvider, "com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy");
        // unified merge policy
        assertCacheMergePolicy(mergePolicyProvider, "com.hazelcast.spi.merge.PutIfAbsentMergePolicy");
        assertCacheMergePolicy(clonedMergePolicyProvider, "com.hazelcast.spi.merge.PutIfAbsentMergePolicy");
    }

    private static void assertInvalidCacheMergePolicy(CacheMergePolicyProvider mergePolicyProvider) {
        try {
            mergePolicyProvider.getMergePolicy("invalid");
            fail("Expected: InvalidConfigurationException: Invalid cache merge policy: invalid");
        } catch (InvalidConfigurationException expected) {
        }
    }

    private static void assertCacheMergePolicy(CacheMergePolicyProvider mergePolicyProvider, String mergePolicyClassname) {
        Object mergePolicy = mergePolicyProvider.getMergePolicy(mergePolicyClassname);
        assertInstanceOfByClassName(mergePolicyClassname, mergePolicy);
    }
}

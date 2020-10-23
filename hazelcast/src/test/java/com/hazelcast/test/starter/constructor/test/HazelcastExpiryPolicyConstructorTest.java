/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.HazelcastExpiryPolicyConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastExpiryPolicyConstructorTest {

    @Test
    public void testConstructor() {
        HazelcastExpiryPolicy policy = new HazelcastExpiryPolicy(3L, 5L, 4L, TimeUnit.SECONDS);
        HazelcastExpiryPolicyConstructor constructor = new HazelcastExpiryPolicyConstructor(HazelcastExpiryPolicy.class);
        HazelcastExpiryPolicy cloned = (HazelcastExpiryPolicy) constructor.createNew(policy);
        assertEquals(policy, cloned);
    }
}

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

package com.hazelcast.quorum;

import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Member implementation for basic map methods nullability tests
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberQuorumServiceNullTest extends AbstractQuorumServiceNullTest {

    private HazelcastInstance instance;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        SplitBrainProtectionConfig sbpc = new SplitBrainProtectionConfig("dummyQuorum", true);
        instance = factory.newHazelcastInstance(new Config().addSplitBrainProtectionConfig(sbpc));
    }

    @Override
    protected HazelcastInstance getDriver() {
        return instance;
    }
}

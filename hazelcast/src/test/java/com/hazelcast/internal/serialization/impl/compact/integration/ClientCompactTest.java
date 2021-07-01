/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.integration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCompactTest extends CompactFormatIntegrationTest {

    HazelcastInstance member1;
    HazelcastInstance member2;

    @Override
    public void setup() {
        member1 = factory.newHazelcastInstance(getConfig());
        member2 = factory.newHazelcastInstance(getConfig());
        instance1 = factory.newHazelcastClient();
        instance2 = factory.newHazelcastClient();
    }

    @Override
    protected void restartCluster() {
        member1.shutdown();
        member2.shutdown();
        member1 = factory.newHazelcastInstance(getConfig());
        member2 = factory.newHazelcastInstance(getConfig());
    }
}

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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.dynamicconfig.DynamicConfigSmokeTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDynamicConfigSmokeTest extends DynamicConfigSmokeTest {

    @Override
    protected HazelcastInstance[] members(int count, Config config) {
        factory = new TestHazelcastFactory(count);
        if (config == null) {
            config = smallInstanceConfig();
        }
        HazelcastInstance[] members = new HazelcastInstance[count];
        for (int i = 0; i < count; i++) {
            members[i] = factory.newHazelcastInstance(config);
        }
        return members;
    }

    @Override
    protected HazelcastInstance driver() {
        return ((TestHazelcastFactory) factory).newHazelcastClient();
    }

    @Override
    @Ignore("Only makes sense to be executed on the member side")
    public void mapConfig_withLiteMemberJoiningLater_isImmediatelyAvailable() {
        super.mapConfig_withLiteMemberJoiningLater_isImmediatelyAvailable();
    }
}

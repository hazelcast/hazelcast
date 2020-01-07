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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDistributedObjectListenerTest extends com.hazelcast.core.DistributedObjectListenerTest {

    @Override
    protected HazelcastInstance newInstance() {
        return hazelcastFactory.newHazelcastClient();
    }

    @Test
    public void distributedObjectsCreatedBack_whenClusterRestart_withSingleNode() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance instance = hazelcastFactory.newHazelcastClient(clientConfig);

        instance.getMap("test");

        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);

        hazelcastFactory.shutdownAllMembers();

        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);
    }

    @Test
    public void distributedObjectsCreatedBack_whenClusterRestart_withMultipleNode() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance instance = hazelcastFactory.newHazelcastClient(clientConfig);

        instance.getMap("test");

        hazelcastFactory.shutdownAllMembers();

        final HazelcastInstance member1 = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance member2 = hazelcastFactory.newHazelcastInstance();

        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);
    }

    @Test
    @Ignore("TODO: Unignore when https://github.com/hazelcast/hazelcast/issues/16374 is fixed")
    public void getDistributedObjects_ShouldNotRecreateProxy_AfterDestroy() {
    }

}

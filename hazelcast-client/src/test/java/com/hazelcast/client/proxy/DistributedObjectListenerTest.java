/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DistributedObjectListenerTest extends HazelcastTestSupport {

    @Before
    @After
    public void cleanup(){
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void destroyedNotReceivedOnClient() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final CountDownLatch createdLatch = new CountDownLatch(1);
        final CountDownLatch destroyedLatch = new CountDownLatch(1);
        client.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {
                createdLatch.countDown();
            }

            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                destroyedLatch.countDown();
            }
        });
        final String name = randomString();
        final ITopic<Object> topic = instance.getTopic(name);
        assertOpenEventually(createdLatch, 10);
        topic.destroy();
        assertOpenEventually(destroyedLatch, 10);
    }

}

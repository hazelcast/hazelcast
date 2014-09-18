/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LifeCycleListenerTest extends HazelcastTestSupport{

    @Test(timeout = 15 * 1000)
    public void testListenerNoDeadLock() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final CountDownLatch latch = new CountDownLatch(1);
        final Config config = new Config();
        config.addListenerConfig(new ListenerConfig(new MyLifecycleListener(latch)));
        factory.newHazelcastInstance(config);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    static class MyLifecycleListener implements LifecycleListener{

        private CountDownLatch latch;

        MyLifecycleListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if( event.getState() == LifecycleEvent.LifecycleState.STARTED){
                Hazelcast.getHazelcastInstanceByName("_hzInstance_1_dev");
                latch.countDown();
            }
        }
    }
}

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

package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static junit.framework.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueListenerTest extends HazelcastTestSupport {

    @Test
    public void testItemListener_addedToQueueConfig() throws InterruptedException {

        int count=2000;

        ItemListenerConfig itemListenerConfig = new ItemListenerConfig();
        itemListenerConfig.setImplementation(new SimpleItemListener(count));
        itemListenerConfig.setIncludeValue(true);

        QueueConfig qConfig = new QueueConfig();
        qConfig.setName("Q");
        qConfig.addItemListenerConfig(itemListenerConfig);

        Config cfg = new Config();
        cfg.addQueueConfig(qConfig);

        HazelcastInstance node1 =  Hazelcast.newHazelcastInstance(cfg);

        for(int i=0; i<count/2; i++) {
            node1.getQueue("Q").put(i);
        }

        HazelcastInstance node2 =  Hazelcast.newHazelcastInstance(cfg);

        for(int i=0; i<count/4; i++) {
            node1.getQueue("Q").put(i);
        }

        List<ItemListenerConfig> configs = qConfig.getItemListenerConfigs();
        SimpleItemListener simpleItemListener = (SimpleItemListener) configs.get(0).getImplementation();

        assertTrue(simpleItemListener.added.await(10, TimeUnit.SECONDS));
    }


    private static class SimpleItemListener implements ItemListener {

        public CountDownLatch added;

        public SimpleItemListener(int CountDown){
            added = new CountDownLatch(CountDown);
        }

        public void itemAdded(ItemEvent itemEvent) {
            added.countDown();
        }

        public void itemRemoved(ItemEvent itemEvent) {
        }
    }

}

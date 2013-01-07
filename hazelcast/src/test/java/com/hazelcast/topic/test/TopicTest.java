/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.topic.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.*;
import com.hazelcast.impl.GroupProperties;
import org.junit.*;
import org.junit.runner.RunWith;
import sun.tools.tree.NewArrayExpression;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;


@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class TopicTest {


    @BeforeClass
    public static void init() throws Exception {

        Hazelcast.shutdownAll();

    }



    @Test
    public void testTopicTotalOrder() throws Exception {
        //final Config cfg = new XmlConfigBuilder("/Users/msk/IdeaProjects/sample/src/main/resources/hazelcast.xml").build();
        final Config cfg = new Config();
        Map<String,TopicConfig> tpCfgs = new HashMap<String, TopicConfig>();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setGlobalOrderingEnabled(true);
        tpCfgs.put("*",topicConfig);
        cfg.setTopicConfigs(tpCfgs);

        final Map<Long,String> stringMap = new HashMap<Long,String>();
        final CountDownLatch countDownLatch = new CountDownLatch(4);
        final CountDownLatch mainLatch = new CountDownLatch(4);

        for(int i = 0; i < 4 ; i++){
            new Thread(new Runnable() {

                public void run() {
                    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
                    ITopic<Long> topic = hazelcastInstance.getTopic("first");
                    final long x = Thread.currentThread().getId();
                    topic.addMessageListener(new MessageListener<Long>() {

                        public void onMessage(Message<Long> message) {
                            String str = stringMap.get(x) + message.getMessageObject().toString();
                            stringMap.put(x, str);
                        }
                    });
                    countDownLatch.countDown();
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for(int j = 0 ; j < 20 ; j++){
                        if(x % 2 == 0)
                            topic.publish((long)j);
                        else
                            topic.publish(Long.valueOf(-1));
                    }
                    mainLatch.countDown();
                }
            },String.valueOf(i)).start();

        }
        mainLatch.await();
        Thread.sleep(500);

        String ref = stringMap.values().iterator().next();
        for (String s : stringMap.values()) {
            if(!ref.equals(s)){
                assertFalse("no total order",true);
                return;
            }
        }
        assertTrue("total order",true);
    }

}

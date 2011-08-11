/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "node-client-applicationContext-hazelcast.xml" })
public class TestClientApplicationContext {

    @Resource
    private HazelcastClient client;

	@Resource(name="instance")
    private HazelcastInstance instance;

	@Resource(name="map1")
    private IMap<Object, Object> map1;

    @Resource(name="map2")
    private IMap<Object, Object> map2;

    @Resource(name="multiMap")
    private MultiMap multiMap;

    @Resource(name="queue")
    private IQueue queue;

    @Resource(name="topic")
    private ITopic topic;

    @Resource(name="set")
    private ISet set;

    @Resource(name="list")
    private IList list;

    @Resource(name="executorService")
    private ExecutorService executorService;

    @Resource(name="idGenerator")
    private IdGenerator idGenerator;

	@Resource(name="atomicNumber")
	private AtomicNumber atomicLong;

    @Resource(name="countDownLatch")
    private ICountDownLatch countDownLatch;

    @Resource(name="semaphore")
    private ISemaphore semaphore;

	@BeforeClass
    @AfterClass
    public static void start(){
        Hazelcast.shutdownAll();
    }

	@Test
	public void testClient() {
		assertNotNull(client);
		final IMap<Object, Object> map = client.getMap("default");
		map.put("Q", "q");
		final IMap<Object, Object> map2 = instance.getMap("default");
		assertEquals("q", map2.get("Q"));
	}

	@Test
    public void testHazelcastInstances() {
        assertNotNull(map1);
        assertNotNull(map2);

        assertNotNull(multiMap);
        assertNotNull(queue);
        assertNotNull(topic);
        assertNotNull(set);
        assertNotNull(list);
        assertNotNull(executorService);
        assertNotNull(idGenerator);
		assertNotNull(atomicLong);
        assertNotNull(countDownLatch);
	    assertNotNull(semaphore);

        assertEquals("map1", map1.getName());
        assertEquals("map2", map2.getName());

        assertEquals("multiMap", multiMap.getName());
        assertEquals("queue", queue.getName());
        assertEquals("topic", topic.getName());
        assertEquals("set", set.getName());
        assertEquals("list", list.getName());
        assertEquals("idGenerator", idGenerator.getName());
        assertEquals("atomicNumber", atomicLong.getName());
        assertEquals("countDownLatch", countDownLatch.getName());
        assertEquals("semaphore", semaphore.getName());
    }

}

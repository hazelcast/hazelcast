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
package com.hazelcast.spring;

import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Ignore
public class TestApplicationContext {

    private Thread[] initialThreads;

    @Before
    public void before() {
        this.initialThreads = getAllThreads();
    }

    @After
    public void after() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
        }
        List<Thread> listOfThreads = new ArrayList<Thread>(Arrays.asList(getAllThreads()));
        for (Thread thread : initialThreads) {
            listOfThreads.remove(thread);
        }
        for (final Thread thread : listOfThreads) {
            if (thread != null) {
                String name = thread.getName();
                if (name != null) {
                    assertFalse(thread.getName().startsWith("hz."));
                }
            }
        }
    }

    @Test
    public void testNodeContext() throws Exception {
        final ClassPathXmlApplicationContext context =
                new ClassPathXmlApplicationContext("classpath:/com/hazelcast/spring/fullcacheconfig-applicationContext-hazelcast.xml");
        final HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        assertNotNull(instance);
        context.destroy();
    }

    @Test
    public void testNodeClientContext() throws Exception {
        final ClassPathXmlApplicationContext context =
                new ClassPathXmlApplicationContext("classpath:/com/hazelcast/spring/node-client-applicationContext-hazelcast.xml");
        final HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        assertNotNull(instance);
        final HazelcastInstance client = (HazelcastInstance) context.getBean("client");
        assertNotNull(client);
        context.destroy();
    }

    static ThreadGroup rootThreadGroup = null;

    public static ThreadGroup getRootThreadGroup() {
        if (rootThreadGroup != null)
            return rootThreadGroup;
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        ThreadGroup ptg;
        while ((ptg = tg.getParent()) != null)
            tg = ptg;
        return tg;
    }

    public static Thread[] getAllThreads() {
        final ThreadGroup root = getRootThreadGroup();
        final ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
        int nAlloc = thbean.getThreadCount();
        int n = 0;
        Thread[] threads;
        do {
            nAlloc *= 2;
            threads = new Thread[nAlloc];
            n = root.enumerate(threads, true);
        } while (n == nAlloc);
        return threads;
    }
}

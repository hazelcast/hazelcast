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

package com.hazelcast.client.standalone;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.standalone.model.MyElement;
import com.hazelcast.client.standalone.model.MyKey;
import com.hazelcast.client.standalone.model.MyPortableElement;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.query.Predicates.*;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapStandaloneTest {

    private static final ClassLoader FILTERING_CLASS_LOADER;

    static HazelcastInstance client;

    static {
        List<String> excludes = Arrays.asList(new String[]{"com.hazelcast.client.standalone.model"});
        FILTERING_CLASS_LOADER = new FilteringClassLoader(excludes, "com.hazelcast");
    }

    @BeforeClass
    public static void init()
            throws Exception {

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            Class<?> configClazz = FILTERING_CLASS_LOADER.loadClass("com.hazelcast.config.Config");
            Object config = configClazz.newInstance();
            Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);

            setClassLoader.invoke(config, FILTERING_CLASS_LOADER);

            Class<?> hazelcastClazz = FILTERING_CLASS_LOADER.loadClass("com.hazelcast.core.Hazelcast");
            Method newHazelcastInstance = hazelcastClazz.getDeclaredMethod("newHazelcastInstance", configClazz);

            newHazelcastInstance.invoke(hazelcastClazz, config);
        } finally {
            thread.setContextClassLoader(tccl);
        }
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(MyPortableElement.FACTORY_ID, new MyPortableElement.Factory());
        client = HazelcastClient.newHazelcastClient(clientConfig);
    }

    public IMap createMap() {
        return client.getMap(randomString());
    }

    @AfterClass
    public static void destroy()
            throws Exception {

        client.shutdown();

        Class<?> hazelcastClazz = FILTERING_CLASS_LOADER.loadClass("com.hazelcast.core.Hazelcast");
        Method shutdownAll = hazelcastClazz.getDeclaredMethod("shutdownAll");
        shutdownAll.invoke(hazelcastClazz);
    }

    @Test
    public void testPut()
            throws Exception {

        MyKey key = new MyKey();
        MyElement element = new MyElement(randomString());

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            IMap<MyKey, MyElement> map = createMap();
            map.put(key, element);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Test
    public void testGet()
            throws Exception {

        IMap<MyKey, MyElement> map = createMap();

        MyKey key = new MyKey();
        MyElement element = new MyElement(randomString());

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            map.put(key, element);
            MyElement result = map.get(key);
            assertEquals(element, result);
        } finally {
            thread.setContextClassLoader(tccl);
        }

    }

    @Test
    public void testRemove()
            throws Exception {

        IMap<MyKey, MyElement> map = createMap();

        MyKey key = new MyKey();
        MyElement element = new MyElement(randomString());

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            map.put(key, element);
            MyElement result = map.remove(key);
            assertEquals(element, result);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Test
    public void testClear()
            throws Exception {

        IMap<MyKey, MyElement> map = createMap();

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            MyKey key = new MyKey();
            MyElement element = new MyElement(randomString());
            map.put(key, element);
            map.clear();
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Test
    public void testPortable_withEntryListenerWithPredicate() throws Exception {
        int key = 1;
        int id = 1;

        IMap<Integer, MyPortableElement> map = createMap();
        Predicate predicate = equal("id", id);
        MyPortableElement element = new MyPortableElement(id);
        final CountDownLatch eventLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<Integer, MyPortableElement>() {
            @Override
            public void onEntryEvent(EntryEvent<Integer, MyPortableElement> event) {
                eventLatch.countDown();
            }
        }, predicate, true);
        map.put(key, element);
        assertOpenEventually(eventLatch);
    }

}

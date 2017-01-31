/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.SqlPredicate;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple test that performs random puts, updates & removes on a map and counts the number of entries
 * and exits (with regards to the value space defined by the predicate) as performed by the map randomizer
 * and as observed on the listener side.
 */
@Ignore("Not a JUnit test")
public class MapListenerTest {

    private static final AtomicInteger ENTRIES, EXITS, ENTRIES_OBSERVED, EXITS_OBSERVED;
    private static final Logger LOGGER = LoggerFactory.getLogger(MapListenerTest.class);
    private static final int AGE_THRESHOLD = 50;

    static {
        System.setProperty("hazelcast.map.entry.filtering.natural.event.types", "true");
        ENTRIES = new AtomicInteger();
        EXITS = new AtomicInteger();
        ENTRIES_OBSERVED = new AtomicInteger();
        EXITS_OBSERVED = new AtomicInteger();
    }

    static class AllListener implements EntryAddedListener<String, Person>, EntryRemovedListener<String, Person>,
            EntryUpdatedListener<String, Person> {

        private static final Logger LOGGER = LoggerFactory.getLogger(AllListener.class);

        @Override
        public void entryAdded(EntryEvent<String, Person> event) {
            ENTRIES_OBSERVED.incrementAndGet();
        }

        @Override
        public void entryRemoved(EntryEvent<String, Person> event) {
            if (event.getValue() != null && event.getOldValue() != null) {
                dumpEvent("exit from removed", event);
            }
            EXITS_OBSERVED.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<String, Person> event) {
            if (event.getOldValue().getAge() > AGE_THRESHOLD &&
                    event.getValue().getAge() <= AGE_THRESHOLD) {
                EXITS_OBSERVED.incrementAndGet();
                dumpEvent("exit", event);
            } else if (event.getOldValue().getAge() <= AGE_THRESHOLD &&
                    event.getValue().getAge() > AGE_THRESHOLD) {
                ENTRIES_OBSERVED.incrementAndGet();
                dumpEvent("entry", event);
            }
        }

        private static void dumpEvent(String qualifier, EntryEvent event) {
            LOGGER.info(qualifier + " " + event);
        }
    }

    static class Person implements Serializable {

        private int age;
        private String name;

        public Person(int age, String name) {
            this.age = age;
            this.name = name;
        }

        public Person(Person p) {
            this.name = p.name;
            this.age = p.age;
        }

        public int getAge() {
            return age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "age=" + age +
                    '}';
        }
    }

    static class MapRandomizer implements Runnable {

        private static final int ACTION_ADD = 0;
        private static final int ACTION_UPDATE_AGE = 1;
        private static final int ACTION_REMOVE = 2;

        final Random random = new Random();
        final IMap<String, Person> map;

        volatile boolean running;

        MapRandomizer(IMap<String, Person> map) {
            this.map = map;
            this.running = true;
        }

        private void act() {
            int action = random.nextInt(10) < 6 ? ACTION_ADD :
                    random.nextInt(10) < 8 ? ACTION_UPDATE_AGE : ACTION_REMOVE;
            switch (action) {
                case ACTION_ADD:
                    addPerson();
                    break;
                case ACTION_UPDATE_AGE:
                    updatePersonAge();
                    break;
                case ACTION_REMOVE:
                    removePerson();
                    break;
            }
        }

        private void addPerson() {
            Person p = new Person(random.nextInt(100), UUID.randomUUID().toString());
            map.put(p.getName(), p);
            if (p.getAge() > AGE_THRESHOLD) {
                ENTRIES.incrementAndGet();
            }
        }

        private void updatePersonAge() {
            if (map.size() > 0) {
                Collection<String> allKeys = map.keySet();
                String key = allKeys.toArray(new String[0])[random.nextInt(map.size())];
                Person p = map.get(key);
                int oldAge = p.getAge();
                p.setAge(p.getAge() + random.nextInt(10) * ((int) Math.pow(-1, random.nextInt(2))));
                if (oldAge > AGE_THRESHOLD && p.getAge() <= AGE_THRESHOLD) {
                    EXITS.incrementAndGet();
                    LOGGER.info("updatePersonAge exit from " + oldAge + " to " + p.getAge());
                } else if (oldAge <= AGE_THRESHOLD && p.getAge() > AGE_THRESHOLD) {
                    ENTRIES.incrementAndGet();
                    LOGGER.info("updatePersonAge entry from " + oldAge + " to " + p.getAge());
                }
                map.put(key, p);
            }
        }

        private void removePerson() {
            if (map.size() > 0) {
                Collection<String> allKeys = map.keySet();
                String key = allKeys.toArray(new String[0])[random.nextInt(map.size())];
                if (map.get(key).getAge() > AGE_THRESHOLD) {
                    EXITS.incrementAndGet();
                }
                map.remove(key);
            }
        }

        @Override
        public void run() {
            while (running) {
                act();
            }
        }

        public void setRunning(boolean running) {
            this.running = running;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // create Hazelcast instance
        Config config = new Config();
        config.setInstanceName("hz-maplistener");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setInterfaces(Arrays.asList(new String[]{"127.0.0.1"}));
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        IMap<String, Person> map = hz.getMap("map");
        MapListener listener = new AllListener();
        map.addEntryListener(listener, new SqlPredicate("age > " + AGE_THRESHOLD), true);

        MapRandomizer mapRandomizer = new MapRandomizer(map);
        Thread t = new Thread(mapRandomizer);
        t.start();

        // let it run for 1 minute
        Thread.sleep(60000);
        mapRandomizer.setRunning(false);

        // assertions
        assertCount(ENTRIES, ENTRIES_OBSERVED, "entries");
        assertCount(EXITS, EXITS_OBSERVED, "exits");

        // dumpMap(map);
        hz.shutdown();
    }

    private static void assertCount(AtomicInteger expected, AtomicInteger observed, String unit) {
        if (expected.get() != observed.get()) {
            LOGGER.error("Actually performed " + expected.get() + " " + unit + ", but observed " + observed.get());
        } else {
            LOGGER.info("Correctly observed " + expected.get() + " " + unit);
        }
    }

    private static void dumpMap(IMap<String, Person> map) {
        LOGGER.info("Map dump follows");
        for (Map.Entry<String, Person> e : map.entrySet()) {
            LOGGER.info(e.getKey() + " > " + e.getValue());
        }
    }
}

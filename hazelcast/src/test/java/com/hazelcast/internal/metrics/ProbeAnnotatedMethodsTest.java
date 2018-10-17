/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeAnnotatedMethodsTest extends AbstractMetricsTest {

    @Before
    public void setupRoots() {
        register(new SubclassWithProbes());
    }

    interface SomeInterface {
        @Probe
        int interfaceMethod();
    }

    private static final class ProbeAnnotatedMethods implements SomeInterface {
        @Override
        public int interfaceMethod() {
            return 10;
        }

        @Probe
        private long argMethod(int x) {
            return 10;
        }

        @Probe
        private void voidMethod() {
        }

        @Probe(name = "mymethod")
        private long method() {
            return 10;
        }

        @Probe
        private byte byteMethod() {
            return 10;
        }

        @Probe
        private short shortMethod() {
            return 10;
        }

        @Probe
        private int intMethod() {
            return 10;
        }

        @Probe
        private long longMethod() {
            return 10;
        }

        @Probe
        private float floatMethod() {
            return 10f;
        }

        @Probe
        private double doubleMethod() {
            return 10d;
        }

        @Probe
        private AtomicLong atomicLongMethod() {
            return new AtomicLong(10);
        }

        @Probe
        private AtomicInteger atomicIntegerMethod() {
            return new AtomicInteger(10);
        }

        @Probe
        private Counter counterMethod() {
            Counter counter = newSwCounter();
            counter.inc(10);
            return counter;
        }

        @Probe
        private Collection<Integer> collectionMethod() {
            ArrayList<Integer> list = new ArrayList<Integer>();
            for (int k = 0; k < 10; k++) {
                list.add(k);
            }
            return list;
        }

        @Probe
        private Map<Integer, Integer> mapMethod() {
            HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
            for (int k = 0; k < 10; k++) {
                map.put(k, k);
            }
            return map;
        }

        @Probe
        private IdentityHashMap<Integer, Integer> subclassMapMethod() {
            IdentityHashMap<Integer, Integer> map = new IdentityHashMap<Integer, Integer>();
            for (int k = 0; k < 10; k++) {
                map.put(k, k);
            }
            return map;
        }

        @Probe
        private static long staticMethod() {
            return 10;
        }

        @Probe
        private long exceptionMethod() {
            throw new IllegalStateException();
        }

        @Probe
        private Long nullMethod() {
            return null;
        }
    }

    @Test
    public void methodReturningNull() {
        assertCollected("ns=foo nullMethod", -1L);
    }

    @Test
    public void methodThrowingAnException() {
        assertCollected("ns=foo exceptionMethod", -1L);
    }

    @Test
    public void methodWithArguments() {
        assertNotCollected("ns=foo argMethod");
    }

    @Test
    public void withCustomName() {
        assertCollected("ns=foo mymethod", 10);
    }

    @Test
    public void methodReturnsVoid() {
        assertNotCollected("ns=foo voidMethod");
    }

    @Test
    public void primitiveByte() {
        assertCollected("ns=foo byteMethod", 10);
    }

    @Test
    public void primitiveShort() {
        assertCollected("ns=foo shortMethod", 10);
    }

    @Test
    public void primitiveInt() {
        assertCollected("ns=foo intMethod", 10);
    }

    @Test
    public void primitiveLong() {
        assertCollected("ns=foo longMethod", 10L);
    }


    @Test
    public void primitiveFloat() {
        assertCollected("ns=foo floatMethod", ProbeUtils.toLong(10d));
    }

    @Test
    public void primitiveDouble() {
        assertCollected("ns=foo doubleMethod", ProbeUtils.toLong(10d));
    }

    @Test
    public void atomicLong() {
        assertCollected("ns=foo atomicLongMethod", 10L);
    }

    @Test
    public void atomicInteger() {
        assertCollected("ns=foo atomicIntegerMethod", 10L);
    }

    @Test
    public void counter() {
        assertCollected("ns=foo counterMethod", 10L);
    }

    @Test
    public void collection() {
        assertCollected("ns=foo collectionMethod", 10L);
    }

    @Test
    public void map() {
        assertCollected("ns=foo mapMethod", 10L);
    }

    @Test
    public void subclass() {
        assertCollected("ns=foo subclassMapMethod", 10L);
    }

    @Test
    public void staticMethod() {
        assertCollected("ns=foo staticMethod", 10L);
    }

    @Test
    public void interfaceMethod() {
        assertCollected("ns=foo interfaceMethod", 10L);
    }

    @Test
    public void superclassWithFieldsAndMethods() {
        assertCollected("inheritedMethod", 10L);
        assertCollected("inheritedField", 10L);
    }

    abstract static class ClassWithProbes {
        @Probe
        int inheritedMethod() {
            return 10;
        }

        @Probe
        int inheritedField = 10;
    }

    private static class SubclassWithProbes extends ClassWithProbes implements MetricsSource {

        ProbeAnnotatedMethods nested = new ProbeAnnotatedMethods();

        @Override
        public void collectAll(CollectionCycle cycle) {
            cycle.switchContext().namespace("foo");
            cycle.collectAll(nested);
        }
    }
}

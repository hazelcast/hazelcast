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

package com.hazelcast.internal.probing;

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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeAnnotatedMethodsTest extends AbstractProbeTest implements ProbeSource {

    @Before
    public void setUp() {
        registry.register(this);
    }

    @Override
    public void probeIn(ProbingCycle cycle) {
        cycle.probe("foo", new ProbeAnnotatedMethods());
        cycle.probe("foo", new SubclassWithProbes());
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
        assertProbed("foo.nullMethod", -1L);
    }

    @Test
    public void methodThrowingAnException() {
        assertNotProbed("foo.exceptionMethod");
    }

    @Test
    public void methodWithArguments() {
        assertNotProbed("foo.argMethod");
    }

    @Test
    public void withCustomName() {
        assertProbed("foo.mymethod", 10);
    }

    @Test
    public void methodReturnsVoid() {
        assertNotProbed("foo.voidMethod");
    }

    @Test
    public void primitiveByte() {
        assertProbed("foo.byteMethod", 10);
    }

    @Test
    public void primitiveShort() {
        assertProbed("foo.shortMethod", 10);
    }

    @Test
    public void primitiveInt() {
        assertProbed("foo.intMethod", 10);
    }

    @Test
    public void primitiveLong() {
        assertProbed("foo.longMethod", 10L);
    }


    @Test
    public void primitiveFloat() {
        assertProbed("foo.floatMethod", ProbeUtils.toLong(10d));
    }

    @Test
    public void primitiveDouble() {
        assertProbed("foo.doubleMethod", ProbeUtils.toLong(10d));
    }

    @Test
    public void atomicLong() {
        assertProbed("foo.atomicLongMethod", 10L);
    }

    @Test
    public void atomicInteger() {
        assertProbed("foo.atomicIntegerMethod", 10L);
    }

    @Test
    public void counter() {
        assertProbed("foo.counterMethod", 10L);
    }

    @Test
    public void collection() {
        assertProbed("foo.collectionMethod", 10L);
    }

    @Test
    public void map() {
        assertProbed("foo.mapMethod", 10L);
    }

    @Test
    public void subclass() {
        assertProbed("foo.subclassMapMethod", 10L);
    }

    @Test
    public void staticMethod() {
        assertProbed("foo.staticMethod", 10L);
    }

    @Test
    public void interfaceMethod() {
        assertProbed("foo.interfaceMethod", 10L);
    }

    @Test
    public void superclassWithGaugeMethods() {
        assertProbed("foo.inheritedMethod", 10L);
        assertProbed("foo.inheritedField", 10L);
    }

    abstract static class ClassWithProbes {
        @Probe
        int inheritedMethod() {
            return 10;
        }

        @Probe
        int inheritedField = 10;
    }

    private static class SubclassWithProbes extends ClassWithProbes {
    }
}

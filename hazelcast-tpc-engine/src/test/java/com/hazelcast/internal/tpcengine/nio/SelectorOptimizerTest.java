/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.channels.Selector;

import static com.hazelcast.internal.tpcengine.nio.SelectorOptimizer.findOptimizableSelectorClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class SelectorOptimizerTest {
    private final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    @Test
    public void testConstructor() {
        assertUtilityConstructor(SelectorOptimizer.class);
    }

    @Test
    public void optimize() throws Exception {
        Selector selector = Selector.open();
        assumeTrue(findOptimizableSelectorClass(selector) != null);
        SelectorOptimizer.SelectionKeysSet keys = SelectorOptimizer.optimize(selector, logger);
        assertNotNull(keys);
    }

    // copied from HazelcastTestSupport
    private static void assertUtilityConstructor(Class clazz) {
        Constructor[] constructors = clazz.getDeclaredConstructors();
        assertEquals("there are more than 1 constructors", 1, constructors.length);

        Constructor constructor = constructors[0];
        int modifiers = constructor.getModifiers();
        assertTrue("access modifier is not private", Modifier.isPrivate(modifiers));

        constructor.setAccessible(true);
        try {
            constructor.newInstance();
        } catch (Exception ignore) {
        }
    }
}

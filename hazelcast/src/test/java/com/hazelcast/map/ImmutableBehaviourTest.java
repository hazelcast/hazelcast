/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Immutable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ImmutableBehaviourTest extends HazelcastTestSupport {

    private final String mapName = randomMapName();

    @Override
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    @Test
    public void testImmutableObjectBinaryFormat() {
        var config = getConfig();
        config.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.BINARY);

        var instance = createHazelcastInstance(config);

        var immutable = new ImmutableObject("test");

        IMap<Integer, ImmutableObject> map = instance.getMap(mapName);

        map.put(1, immutable);

        var get = map.get(1);

        assertTrue(immutable != get);


        var old = map.put(1, new ImmutableObject("different"));

        assertTrue(immutable != old);
        assertTrue(get != old);


    }


    @Test
    public void testImmutableObjectObjectFormat() {
        var config = getConfig();
        config.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.OBJECT);

        var instance = createHazelcastInstance(config);

        var immutable = new ImmutableObject("test");

        IMap<Integer, ImmutableObject> map = instance.getMap(mapName);

        map.put(1, immutable);

        assertTrue(immutable == map.get(1));


        var old = map.put(1, new ImmutableObject("different"));

        assertTrue(immutable == old);
    }

    @Test
    public void testMutableObjectObjectFormat() {
        var config = getConfig();
        config.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.OBJECT);

        var instance = createHazelcastInstance(config);

        var mutable = new MutableObject("test");

        IMap<Integer, MutableObject> map = instance.getMap(mapName);

        map.put(1, mutable);

        var get = map.get(1);

        assertTrue(mutable != get);


        var old = map.put(1, new MutableObject("different"));

        assertTrue(mutable != old);
        assertTrue(get != old);
    }


    record ImmutableObject(String name) implements Immutable {
    }

    record MutableObject(String name) {
    }


}

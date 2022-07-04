/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexSplitBrainTest extends SplitBrainTestSupport {

    private final String mapName = randomMapName();

    private String key;
    private ValueObject value;

    @Override
    protected int[] brains() {
        return new int[]{1, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);
        key = generateKeyOwnedBy(instances[0]);
        value = new ValueObject(key);

        final IMap<String, ValueObject> map1 = instances[0].getMap(mapName);
        final IMap<String, ValueObject> map2 = instances[1].getMap(mapName);

        map1.put(key, value);
        assertNotNull("Entry should exist in map2 before split", map2.get(key));
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        final IMap<String, ValueObject> map1 = firstBrain[0].getMap(mapName);
        final IMap<String, ValueObject> map2 = secondBrain[0].getMap(mapName);
        map1.remove(key);
        assertNotNull("Entry should exist in map2 during split", map2.get(key));
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        final IMap<String, ValueObject> map1 = instances[0].getMap(mapName);
        final IMap<String, ValueObject> map2 = instances[1].getMap(mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull("Entry should exist in map1 after merge", map1.get(key));
            }
        }, 15);
        map1.remove(key);
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                Predicate predicate = Predicates.equal("id", value.getId());
                Collection<ValueObject> values = map1.values(predicate);
                assertThat(values, empty());

                values = map2.values(predicate);
                assertThat(values, empty());
            }
        }, 5);
    }

    @Override
    protected Config config() {
        Config config = super.config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "id"));
        return config;
    }

    private static class ValueObject implements DataSerializable {

        private String id;

        ValueObject() {
        }

        ValueObject(String id) {
            this.id = id;
        }

        public String getId() {
            return this.id;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(this.id);
        }

        public void readData(ObjectDataInput in) throws IOException {
            this.id = in.readString();
        }
    }
}

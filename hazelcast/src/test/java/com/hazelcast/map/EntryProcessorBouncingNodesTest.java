/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Creates a map that is used to test data consistency while nodes are joining and leaving the cluster.
 * <p>
 * The basic idea is pretty simple. We'll add a number to a list for each key in the IMap. This allows us to verify whether
 * the numbers are added in the correct order and also whether there's any data loss as nodes leave or join the cluster.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryProcessorBouncingNodesTest extends HazelcastTestSupport {

    private static final int ENTRIES = 50;
    private static final int ITERATIONS = 100;
    private static final String MAP_NAME = "entryProcessorBouncingNodesTestMap";

    @Parameters(name = "withPredicate={0}, withIndex={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, false}, {true, false}, {true, true}
        });
    }

    @Parameter
    public boolean withPredicate;

    @Parameter
    public boolean withIndex;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule
            .with(getConfig())
            .clusterSize(3)
            .driverCount(1)
            .driverType(BounceTestConfiguration.DriverType.ALWAYS_UP_MEMBER)
            .build();

    @Before
    public void setUp() {
        HazelcastInstance instance = bounceMemberRule.getSteadyMember();
        IMap<Integer, ListHolder> map = instance.getMap(MAP_NAME);
        // initialize the list synchronously to ensure the map is correctly initialized
        InitMapProcessor initProcessor = new InitMapProcessor();
        for (int i = 0; i < ENTRIES; ++i) {
            map.executeOnKey(i, initProcessor);
        }
        assertEquals(ENTRIES, map.size());
    }

    @Test
    public void testEntryProcessorWhileTwoNodesAreBouncing() {
        // now, with nodes joining and leaving the cluster concurrently, start adding numbers to the lists
        final ListHolder expected = new ListHolder();
        int iteration = 0;

        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        final IMap<Integer, ListHolder> map = steadyMember.getMap(MAP_NAME);
        while (iteration < ITERATIONS) {
            IncrementProcessor processor = new IncrementProcessor(iteration);
            expected.add(iteration);
            for (int i = 0; i < ENTRIES; ++i) {
                if (withPredicate) {
                    EntryObject eo = new PredicateBuilder().getEntryObject();
                    Predicate keyPredicate = eo.key().equal(i);
                    map.executeOnEntries(processor, keyPredicate);
                } else {
                    map.executeOnKey(i, processor);
                }
            }
            ++iteration;
        }

        for (int i = 0; i < ENTRIES; i++) {
            final int index = i;
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    ListHolder holder = map.get(index);
                    String errorText = String.format("Each ListHolder should contain %d entries.\nInvalid list holder content:\n%s\n", ITERATIONS, holder.toString());
                    assertEquals(errorText, ITERATIONS, holder.size());
                    for (int i = 0; i < ITERATIONS; i++) {
                        assertEquals(i, holder.get(i));
                    }
                }
            });
        }
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setBackupCount(2);
        if (withIndex) {
            mapConfig.addMapIndexConfig(new MapIndexConfig("__key", true));
        }
        return config;
    }

    private static class InitMapProcessor extends AbstractEntryProcessor<Integer, ListHolder> {

        @Override
        public Object process(Map.Entry<Integer, ListHolder> entry) {
            entry.setValue(new ListHolder());
            return null;
        }
    }

    private static class IncrementProcessor extends AbstractEntryProcessor<Integer, ListHolder> {

        private final int nextVal;

        private IncrementProcessor(int nextVal) {
            this.nextVal = nextVal;
        }

        @Override
        public Object process(Map.Entry<Integer, ListHolder> entry) {
            ListHolder holder = entry.getValue();
            holder.add(nextVal);
            entry.setValue(holder);
            return null;
        }
    }

    private static class ListHolder implements DataSerializable {

        private List<Integer> list = new ArrayList<Integer>();
        private int size;

        public ListHolder() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(list.size());
            for (Integer value : list) {
                out.writeInt(value);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            size = in.readInt();
            list = new ArrayList<Integer>(size);
            for (int i = 0; i < size; i++) {
                list.add(in.readInt());
            }
        }

        public void add(int value) {
            // EPs should be idempotent if consistency for such type of operations required
            if (!list.contains(value)) {
                list.add(value);
                size++;
            }
        }

        public int get(int index) {
            return list.get(index);
        }

        public int size() {
            return size;
        }

        @Override
        public String toString() {
            return Arrays.toString(list.toArray());
        }
    }
}

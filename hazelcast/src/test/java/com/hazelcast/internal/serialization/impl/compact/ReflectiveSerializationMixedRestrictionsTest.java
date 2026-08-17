/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.addToCompactSerializationAllowList;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ReflectiveSerializationMixedRestrictionsTest {

    private static final String OBJECT_MAP_NAME = "object-map";
    private static final int ENTRY_COUNT = 100;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    public static class IntWrapper {
        private int n;

        public IntWrapper(int n) {
            this.n = n;
        }
    }

    private Config getUnrestrictedConfig() {
        Config baseConfig = smallInstanceConfigWithoutJetAndMetrics();
        baseConfig.addMapConfig(new MapConfig(OBJECT_MAP_NAME).setInMemoryFormat(InMemoryFormat.OBJECT));
        addToCompactSerializationAllowList(baseConfig.getSerializationConfig(), IntWrapper.class);
        return baseConfig;
    }

    private Config getRestrictedConfig() {
        Config config = getUnrestrictedConfig();
        config.getSerializationConfig().getCompactSerializationConfig().setZeroConfigFilter(getRestrictionFilter());
        return config;
    }

    private JavaSerializationFilterConfig getRestrictionFilter() {
        return new JavaSerializationFilterConfig().setBlacklist(new ClassFilter().addClasses(IntWrapper.class.getName()));
    }

    @Test
    public void testMixedClientRestrictions() {
        HazelcastInstance[] members = factory.newInstances(getUnrestrictedConfig(), 3);
        waitAllForSafeState(members);
        ClientConfig restrictedConfig = new ClientConfig();
        restrictedConfig.getSerializationConfig().getCompactSerializationConfig()
                        .setZeroConfigFilter(getRestrictionFilter());
        HazelcastInstance restrictedClient = factory.newHazelcastClient(restrictedConfig);
        ClientConfig unrestrictedConfig = new ClientConfig();
        addToCompactSerializationAllowList(unrestrictedConfig.getSerializationConfig(), IntWrapper.class);
        HazelcastInstance unrestrictedClient = factory.newHazelcastClient(unrestrictedConfig);

        // Restricted client cannot directly write the restricted class
        assertThatThrownBy(() -> restrictedClient.getMap("map").set(0, new IntWrapper(0))).rootCause().isInstanceOf(
                ReflectiveCompactSerializationUnsupportedException.class);

        // Unrestricted class can continue to write class
        unrestrictedClient.getMap("map").set(0, new IntWrapper(0));

        // Restricted client can still read existing data but it is deserialized to GenericRecord
        assertThat(restrictedClient.getMap("map").get(0)).isEqualTo(
                GenericRecordBuilder.compact(IntWrapper.class.getName()).setInt32("n", 0).build());
    }

    @Test
    public void testIntroducingRestrictedMember() {
        Config unrestrictedConfig = getUnrestrictedConfig();
        int nodeCount = 3;
        HazelcastInstance[] members = factory.newInstances(unrestrictedConfig, nodeCount);
        assertThat(members[0].getCluster().getMembers()).hasSize(3);
        IMap<Integer, IntWrapper> data = members[0].getMap(OBJECT_MAP_NAME);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            data.set(i, new IntWrapper(i));
        }
        Config restrictedConfig = getRestrictedConfig();
        members[nodeCount - 1].shutdown();
        waitAllForSafeState(members);
        members[nodeCount - 1] = factory.newHazelcastInstance(restrictedConfig);
        waitAllForSafeState(members);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            for (int j = 0; j < nodeCount - 1; j++) {
                assertThat(members[j].getMap(OBJECT_MAP_NAME).get(i)).isInstanceOf(IntWrapper.class);
            }
            assertThat(members[nodeCount - 1].getMap(OBJECT_MAP_NAME).get(i)).isInstanceOf(DeserializedGenericRecord.class);
        }
    }
}

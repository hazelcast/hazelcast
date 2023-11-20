/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobInvocationObserver;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class MemberPruningTest extends SqlTestSupport {

    public static final int MEMBER_COUNT = 5;
    private static final JobInvocationObserverImpl JOB_INVOCATION_OBSERVER = new JobInvocationObserverImpl();
    private static final Random RAND = new Random();
    private KeyPojo key;
    private HazelcastInstance partitionOwnerMember;
    private String mapName;

    @BeforeClass
    public static void setupClass() {
        initialize(MEMBER_COUNT, defaultInstanceConfigWithJetEnabled());
        getJetServiceBackend(instance())
                .getJobCoordinationService()
                .registerInvocationObserver(JOB_INVOCATION_OBSERVER);
    }

    @Before
    public void setup() {
        mapName = "test_" + randomName();
        createMapping(mapName, KeyPojo.class, Integer.class);
    }

    @Test
    public void testSimpleSelect() {
        configureMapWithAttributes(mapName, "stringField");

        populateMap();
        assertRowsAnyOrder(instance(),
                "SELECT this FROM " + mapName + " WHERE stringField = ?",
                Collections.singletonList(key.getStringField()),
                Collections.singletonList(new Row(Integer.MAX_VALUE)));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @Test
    public void testSimpleSelectWithNonPartitioningKey() {
        populateMap();
        assertRowsAnyOrder(instance(),
                "SELECT this FROM " + mapName + " WHERE longField = ?",
                Collections.singletonList(key.getLongField()),
                Collections.singletonList(new Row(Integer.MAX_VALUE)));

        assertInvokedOnlyOnMembers(instances());
    }

    @Test
    public void testSelectMultiplePartitioningAttributes() {
        configureMapWithAttributes(mapName, "stringField", "longField");

        partitionOwnerMember = getRandomMember();
        Member localMember = partitionOwnerMember.getCluster().getLocalMember();
        PartitionService partitionService = partitionOwnerMember.getPartitionService();
        String stringField;
        long longField;
        Partition partition;
        do {
            stringField = randomString();
            longField = RAND.nextLong();
            partition = partitionService.getPartition(new Object[]{stringField, longField});
        } while (!localMember.equals(partition.getOwner()));
        key = new KeyPojo(stringField, RAND.nextInt(), longField);
        instance().getMap(mapName).set(key, Integer.MAX_VALUE);

        assertRowsAnyOrder(instance(),
                "SELECT this FROM " + mapName + " WHERE stringField = ? AND longField = ?",
                asList(key.getStringField(), key.getLongField()),
                Collections.singletonList(new Row(Integer.MAX_VALUE)));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @Test
    public void testSelectWithUnionAll() {
        configureMapWithAttributes(mapName, "stringField");

        String otherMapName = "test_other_" + randomName();
        createMapping(otherMapName, KeyPojo.class, Integer.class);
        configureMapWithAttributes(otherMapName, "stringField");

        populateMap();
        KeyPojo otherKey = new KeyPojo(
                generateKeyOwnedBy(partitionOwnerMember),
                RAND.nextInt(),
                Long.MAX_VALUE
        );
        instance().getMap(otherMapName).set(otherKey, Integer.MIN_VALUE);

        assertRowsAnyOrder(instance(),
                "(SELECT this FROM " + mapName + " WHERE stringField = ?) "
                        + "UNION ALL "
                        + "(SELECT this FROM " + otherMapName + " WHERE stringField = ?)",
                List.of(key.getStringField(), otherKey.getStringField()),
                List.of(new Row(Integer.MAX_VALUE), new Row(Integer.MIN_VALUE)));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @Test
    public void testSelectWithConjunctionAndArbitraryFilter() {
        configureMapWithAttributes(mapName, "stringField");

        populateMap();
        assertRowsAnyOrder(instance(),
                "SELECT this FROM " + mapName + " WHERE stringField = ? AND 1 = 1",
                Collections.singletonList(key.getStringField()),
                Collections.singletonList(new Row(Integer.MAX_VALUE)));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @After
    public void teardown() {
        instance().getMap(mapName).destroy();
    }

    private HazelcastInstance getRandomMember() {
        return instances()[RAND.nextInt(MEMBER_COUNT)];
    }

    private void populateMap() {
        partitionOwnerMember = getRandomMember();
        key = new KeyPojo(
                generateKeyOwnedBy(partitionOwnerMember),
                RAND.nextInt(),
                Long.MAX_VALUE
        );
        instance().getMap(mapName).set(key, Integer.MAX_VALUE);
    }

    private void configureMapWithAttributes(String mapName, String... attributes) {
        MapConfig mc = new MapConfig(mapName);
        for (String attribute : attributes) {
            mc.getPartitioningAttributeConfigs()
                    .add(new PartitioningAttributeConfig(attribute));
        }
        instance().getConfig().addMapConfig(mc);
    }

    private void assertInvokedOnlyOnMembers(HazelcastInstance... members) {
        Set<Address> expectedMembers = Arrays.stream(members)
                .map(JetTestSupport::getAddress)
                .collect(Collectors.toSet());
        assertEquals(expectedMembers, JOB_INVOCATION_OBSERVER.getMembers());
    }

    private static class JobInvocationObserverImpl implements JobInvocationObserver {
        private Set<MemberInfo> members;

        @Override
        public void onLightJobInvocation(long jobId, Set<MemberInfo> members, DAG dag, JobConfig jobConfig) {
            this.members = members;
        }

        public Set<Address> getMembers() {
            return members.stream().map(MemberInfo::getAddress).collect(toSet());
        }
    }

    public static class KeyPojo implements Serializable {

        private String stringField;
        private int intField;
        private long longField;

        @Override
        public String toString() {
            return "KeyPojo{" + "stringField='" + stringField + '\'' + ", intField=" + intField + ", longField=" + longField
                    + '}';
        }

        KeyPojo(String stringField, int intField, long longField) {
            this.stringField = stringField;
            this.intField = intField;
            this.longField = longField;
        }

        public String getStringField() {
            return stringField;
        }

        public void setStringField(String stringField) {
            this.stringField = stringField;
        }

        public int getIntField() {
            return intField;
        }

        public void setIntField(int intField) {
            this.intField = intField;
        }

        public long getLongField() {
            return longField;
        }

        public void setLongField(long longField) {
            this.longField = longField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyPojo keyPojo = (KeyPojo) o;
            return getIntField() == keyPojo.getIntField() && getLongField() == keyPojo.getLongField() && Objects.equals(
                    getStringField(), keyPojo.getStringField());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getStringField(), getIntField(), getLongField());
        }
    }
}

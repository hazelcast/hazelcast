/*
 * Copyright 2024 Hazelcast Inc.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static java.util.Arrays.asList;

public class MemberPruningTest extends SqlEndToEndTestSupport {

    public static final int MEMBER_COUNT = 5;
    private static final Random RAND = new Random();
    private String key;
    private HazelcastInstance partitionOwnerMember;
    private String mapName;

    @BeforeClass
    public static void setupClass() {
        initialize(MEMBER_COUNT, defaultInstanceConfigWithJetEnabled());
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
                Collections.singletonList(key),
                Collections.singletonList(new Row(0)));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @Test
    public void testSimpleSelectWithNonPartitioningKey() {
        populateMap();
        assertRowsAnyOrder(instance(),
                "SELECT this FROM " + mapName + " WHERE longField = ?",
                Collections.singletonList(Long.MAX_VALUE),
                Collections.singletonList(new Row(0)));

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
        key = stringField;
        instance().getMap(mapName).set(new KeyPojo(key, RAND.nextInt(), longField), Integer.MAX_VALUE);

        assertRowsAnyOrder(instance(),
                "SELECT this FROM " + mapName + " WHERE stringField = ? AND longField = ?",
                asList(key, longField),
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
                List.of(key, otherKey.getStringField()),
                List.of(new Row(0), new Row(Integer.MIN_VALUE)));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @Test
    public void testSelectWithConjunctionAndArbitraryFilter() {
        configureMapWithAttributes(mapName, "stringField");

        populateMap();
        assertRowsAnyOrder(instance(),
                "SELECT this FROM " + mapName + " WHERE stringField = ? AND 1 = 1",
                Collections.singletonList(key),
                Collections.singletonList(new Row(0)));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @Test
    public void testSelectWithOrderBy() {
        configureMapWithAttributes(mapName, "stringField");
        populateMap(10);
        assertRowsOrdered(instance(),
                "SELECT this FROM " + mapName
                        + " WHERE stringField = '"  + key
                        + "' ORDER BY this",
                rows(1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        assertInvokedOnlyOnMembers(instance(), partitionOwnerMember);
    }

    @After
    @Override
    public void teardown() {
        instance().getMap(mapName).destroy();
    }

    private HazelcastInstance getRandomMember() {
        return instances()[RAND.nextInt(MEMBER_COUNT)];
    }

    private void populateMap() {
        populateMap(1);
    }

    private void populateMap(int entryCount) {
        partitionOwnerMember = getRandomMember();
        key = generateKeyOwnedBy(partitionOwnerMember);
        for (int i = 0; i < entryCount; i++) {
            instance().getMap(mapName).set(
                new KeyPojo(
                    key,
                    RAND.nextInt(),
                    Long.MAX_VALUE
                ), i);
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

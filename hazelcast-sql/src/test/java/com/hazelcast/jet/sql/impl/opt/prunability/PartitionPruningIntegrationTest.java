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

package com.hazelcast.jet.sql.impl.opt.prunability;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * In the future this test will perform full cycle of testing, verifying that only desired nodes are participating in
 * the job execution.
 */
@RunWith(HazelcastSerialClassRunner.class)
public class PartitionPruningIntegrationTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(6, null);
    }

    @Before
    public void init() {
        instance().getConfig().addMapConfig(new MapConfig("test2").setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("comp1"),
                new PartitioningAttributeConfig("comp2")
        )));
        instance().getConfig().addMapConfig(new MapConfig("testMap").setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("comp1"),
                new PartitioningAttributeConfig("comp2")
        )));
        instance().getSql().execute("CREATE MAPPING test1 TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='varchar')");
        instance().getSql().execute("CREATE MAPPING test2 TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + KeyObj.class.getName() + "')");

        instance().getSql().execute("SINK INTO test1 VALUES (1, 'v1')");
        instance().getSql().execute("SINK INTO test1 VALUES (2, 'v2')");

        instance().getSql().execute("SINK INTO test2 VALUES (1, 1, 100, 'v1')");
        instance().getSql().execute("SINK INTO test2 VALUES (2, 2, 200, 'v2')");
        instance().getSql().execute("SINK INTO test2 VALUES (3, 3, 300, 'v3')");
        instance().getSql().execute("SINK INTO test2 VALUES (4, 4, 300, 'v3')");
        instance().getSql().execute("SINK INTO test2 VALUES (5, 5, 300, 'v3')");
        instance().getSql().execute("SINK INTO test2 VALUES (6, 6, 300, 'v3')");

        instance().getSql().execute("CREATE MAPPING test3 EXTERNAL NAME testMap ("
                + "c1 BIGINT EXTERNAL NAME \"__key.comp1\","
                + "c2 BIGINT EXTERNAL NAME \"__key.comp2\","
                + "c3 BIGINT EXTERNAL NAME \"__key.comp3\","
                + "this VARCHAR"
                + ") TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + KeyObj.class.getName() + "')");

        instance().getSql().execute("SINK INTO test3 VALUES (1, 1, 1, 'hello')");
    }

    @Test
    public void test_simpleKeyAndEmptyMapPruned() {
        createMapping("test4", Integer.class, Integer.class);
        instance().getMap("test4");
        assertRowsAnyOrder("SELECT this FROM test4 WHERE __key = ? AND this IS NOT NULL", List.of(1), rows(0));
    }

    @Test
    public void test_simpleKeyPruned() {
        assertRowsAnyOrder("SELECT this FROM test1 WHERE __key = ? AND this = 'v1'", List.of(1), rows(1, "v1"));
    }

    @Test
    public void test_simpleKeyNotPruned() {
        assertRowsAnyOrder("SELECT this FROM test1 WHERE __key > 0 AND this = 'v1'", rows(1, "v1"));
    }

    @Test
    public void test_simpleKeyNullNotPruned() {
        assertRowsAnyOrder("SELECT this FROM test1 WHERE __key = ? AND this = 'v1'", Arrays.asList(new Object[1]), rows(1));
    }

    @Test
    public void test_simpleKeyPrunedOrderBy() {
        assertRowsAnyOrder("SELECT this FROM test2 WHERE comp1 = 1 AND comp2 = ? ORDER BY comp2", List.of(1), rows(1, "v1"));
    }

    @Test
    public void test_compoundKeyPruned() {
        assertRowsAnyOrder("SELECT this FROM test2 WHERE comp1 = 1 AND comp2 = 1 AND comp3 = 100", rows(1, "v1"));
    }

    @Test
    public void test_compoundKeyWithNullAttribute() {
        // currently this query is pruned but null attribute value for AttributePartitionStrategy generally is not allowed
        assertRowsAnyOrder("SELECT this FROM test2 WHERE comp1 = ? AND comp2 = 1 AND comp3 = 100",
                Arrays.asList(new Object[1]), rows(1));
    }

    @Test
    public void test_compoundKeyNotPruned() {
        assertRowsAnyOrder("SELECT this FROM test2 WHERE comp1 = 1 AND comp3 = 100", rows(1, "v1"));
    }

    @Test
    public void test_renamingKeyPruned() {
        assertRowsAnyOrder("SELECT this FROM hazelcast.public.test3 WHERE c1 = 1 AND c2 = 1", rows(1, "hello"));
    }

    @Test
    public void test_renamingKeyNotPruned() {
        assertRowsAnyOrder("SELECT this FROM hazelcast.public.test3 WHERE c2 = 1 AND c3 = 1", rows(1, "hello"));
    }

    @Test
    public void test_partitionAwareKeyNonMappedShouldBePruned() {
        instance().getSql().execute("CREATE MAPPING test4 TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + PAKey.class.getName() + "')");

        instance().getMap("test4").put(new PAKey(1L, "1"), "v1");

        assertRowsAnyOrder("SELECT this FROM test4 WHERE __key = ? AND this = 'v1'", singletonList(new PAKey(1L, "1")), rows(1, "v1"));
    }

    @Test
    public void test_partitionAwareKeyMappedShouldNotBePruned() {
        instance().getSql().execute("CREATE MAPPING test5 ("
                + "id BIGINT EXTERNAL NAME \"__key.id\", "
                + "name VARCHAR EXTERNAL NAME \"__key.name\", "
                + "this VARCHAR"
                + ") TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + PAKey.class.getName() + "')");
        instance().getMap("test5").put(new PAKey(1L, "1"), "v1");

        assertRowsAnyOrder("SELECT this FROM test5 WHERE id = 1 AND name = '1' AND this = 'v1'", rows(1, "v1"));
    }

    @Test
    public void test_compundSingleFieldNullKeyShouldNotBePruned() {
        String mapName = randomName();
        instance().getConfig().addMapConfig(new MapConfig(mapName).setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("nestedKey")
        )));
        var paKey = new PAKey(1L, "one");
        IMap<KeyWithPAField, String> map = instance().getMap(mapName);
        map.put(new KeyWithPAField(paKey), "oneValue");

        createMapping(mapName, KeyWithPAField.class, String.class);

        assertRowsAnyOrder("SELECT this FROM " + mapName + " WHERE nestedKey = ?", Arrays.asList(new Object[1]), rows(1));
    }

    @Test
    public void test_keyWithNestedPartitionAwareKeyShouldBePruned() {
        String mapName = randomName();
        instance().getConfig().addMapConfig(new MapConfig(mapName).setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("nestedKey")
        )));
        var paKey = new PAKey(1L, "one");
        IMap<KeyWithPAField, String> map = instance().getMap(mapName);
        map.put(new KeyWithPAField(paKey), "oneValue");

        createMapping(mapName, KeyWithPAField.class, String.class);

        // it can happen by accident that the test passes if partition id happens correct, but usually it wil fail
        assertRowsAnyOrder("SELECT this FROM " + mapName + " WHERE nestedKey = ?", List.of(paKey), rows(1, "oneValue"));
    }

    @Test
    public void test_partitionAwareKeyWithNestedPartitionAwareKeyShouldBePruned() {
        String mapName = randomName();
        instance().getConfig().addMapConfig(new MapConfig(mapName).setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("nestedKey")
        )));
        var paKey = new PAKey(1L, "one");
        IMap<PAKeyWithPAField, String> map = instance().getMap(mapName);
        map.put(new PAKeyWithPAField(paKey), "oneValue");

        createMapping(mapName, PAKeyWithPAField.class, String.class);

        // it can happen by accident that the test passes if partition id happens correct, but usually it wil fail
        assertRowsAnyOrder("SELECT this FROM " + mapName + " WHERE nestedKey = ?", List.of(paKey), rows(1, "oneValue"));
    }

    @Test
    public void test_partitionAwarePartitionAwareKeyShouldBePruned() {
        String mapName = randomName();
        var paKey = new PAKey(1L, "one");
        IMap<PAKeyWithPAField, String> map = instance().getMap(mapName);
        map.put(new PAKeyWithPAField(paKey), "oneValue");

        createMapping(mapName, PAKeyWithPAField.class, String.class);

        // it can happen by accident that the test passes if partition id happens correct, but usually it wil fail
        assertRowsAnyOrder("SELECT this FROM " + mapName + " WHERE __key = ? and this = 'oneValue'", List.of(new PAKeyWithPAField(paKey)), rows(1, "oneValue"));
    }

    @Test
    public void test_scanPruningWithoutMemberPruning() {
        // complicated query that should not be eligible for member pruning
        // but at least one side of the join should execute full scan that is eligible for scan partition pruning
        // - it does not matter if nested loops or hash join is used.
        assertRowsAnyOrder("SELECT max(b.this), b.comp2 " +
                "FROM test2 a join test2 b on a.comp2 = b.comp2 " +
                "WHERE a.comp1 = 1 AND a.comp2 = ? and b.comp1 = 1 AND b.comp2 = ? " +
                "GROUP BY b.comp2 ORDER BY b.comp2", List.of(1, 1), rows(2, "v1", 1L));
    }

    public static class PAKey implements Serializable, PartitionAware<String>, Comparable<PAKey> {
        public Long id;
        public String name;

        public PAKey() { }

        public PAKey(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String getPartitionKey() {
            return "hello";
        }

        @Override
        public int compareTo(@NotNull final PAKey o) {
            return hashCode() - o.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PAKey paKey = (PAKey) o;
            return Objects.equals(id, paKey.id) && Objects.equals(name, paKey.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    public static class KeyWithPAField implements Serializable {
        private PAKey nestedKey;

        public KeyWithPAField() {
        }

        public KeyWithPAField(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        public PAKey getNestedKey() {
            return nestedKey;
        }

        public void setNestedKey(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyWithPAField that = (KeyWithPAField) o;
            return Objects.equals(nestedKey, that.nestedKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nestedKey);
        }
    }

    public static class PAKeyWithPAField implements Serializable, PartitionAware<PAKey>, Comparable<PAKeyWithPAField> {
        private PAKey nestedKey;

        public PAKeyWithPAField() {
        }

        public PAKeyWithPAField(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        @Override
        public PAKey getPartitionKey() {
            // this is a very special border case but theoretically valid
            return nestedKey;
        }

        public PAKey getNestedKey() {
            return nestedKey;
        }

        public void setNestedKey(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyWithPAField that = (KeyWithPAField) o;
            return Objects.equals(nestedKey, that.nestedKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nestedKey);
        }

        @Override
        public int compareTo(PAKeyWithPAField o) {
            return nestedKey.compareTo(o.nestedKey);
        }
    }

    public static class KeyObj implements Serializable {
        public Long comp1;
        public Long comp2;
        public Long comp3;

        public KeyObj() {

        }

        public KeyObj(final Long comp1, final Long comp2, final Long comp3) {
            this.comp1 = comp1;
            this.comp2 = comp2;
            this.comp3 = comp3;
        }
    }
}

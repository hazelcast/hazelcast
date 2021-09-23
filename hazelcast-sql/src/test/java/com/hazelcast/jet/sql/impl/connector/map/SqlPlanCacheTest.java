/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.PersonId;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlPlanCacheTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_tableName() {
        createMapping("map1", "m", "id", PersonId.class, "varchar");
        sqlService.execute("SELECT * FROM map1");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("map2", "m", "id", PersonId.class, "varchar");
        sqlService.execute("DROP MAPPING map1");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_mapName() {
        createMapping("map", "m1", "id", PersonId.class, "varchar");
        sqlService.execute("SELECT * FROM map");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("map", "m2", "id", PersonId.class, "varchar");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_fieldList() {
        createMapping("map", "m", "id1", PersonId.class, "varchar");
        sqlService.execute("SELECT * FROM map");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("map", "m", "id2", PersonId.class, "varchar");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_keyDescriptor() {
        createMapping("map", "m", "id", PersonId.class, "varchar");
        sqlService.execute("SELECT * FROM map");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("map", "m", "id", 1, 2, "varchar");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_valueDescriptor() {
        createMapping("map", "m", "id", PersonId.class, "varchar");
        sqlService.execute("SELECT * FROM map");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("map", "m", "id", PersonId.class, JSON_FLAT_FORMAT);
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_conflictingSchemas() {
        createMapping("map", "m", "id", PersonId.class, "varchar");
        sqlService.execute("SELECT * FROM map");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        instance().getMap("map").put(1, "1");
        sqlService.execute("DROP MAPPING map");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_index() {
        IMap<Object, Object> map = instance().getMap("m");

        createMapping("map", map.getName(), "id", PersonId.class, "varchar");
        String indexName = randomName();

        map.addIndex(new IndexConfig(IndexType.SORTED, "__key.id").setName(indexName));
        sqlService.execute("SELECT * FROM map ORDER BY id");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        mapContainer(map).getIndexes().destroyIndexes();
        map.addIndex(new IndexConfig(IndexType.HASH, "__key.id").setName(indexName));

        assertTrueEventually(() -> assertThat(planCache(instance()).size()).isZero());
    }

    @Test
    public void test_dmlCaching() {
        createMapping("map", "m", "id", PersonId.class, "varchar");
        sqlService.execute("INSERT INTO map (id, this) VALUES(0, 'value-0')");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        sqlService.execute("SINK INTO map (id, this) VALUES(0, 'value-0')");
        assertThat(planCache(instance()).size()).isEqualTo(2);

        sqlService.execute("UPDATE map SET this = 'value-1' WHERE id = 0");
        assertThat(planCache(instance()).size()).isEqualTo(3);

        sqlService.execute("DELETE FROM map WHERE id = 0");
        assertThat(planCache(instance()).size()).isEqualTo(4);

        sqlService.execute("DROP MAPPING map");
        assertThat(planCache(instance()).size()).isZero();
    }

    @SuppressWarnings("SameParameterValue")
    private static void createMapping(
            String name,
            String mapName,
            String keyFieldName,
            Class<?> keyClass,
            String valueFormat
    ) {
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " EXTERNAL NAME " + mapName + " ("
                + keyFieldName + " INT EXTERNAL NAME \"__key.id\""
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + keyClass.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + valueFormat + '\''
                + ")"
        );
    }

    @SuppressWarnings("SameParameterValue")
    private static void createMapping(
            String name,
            String mapName,
            String keyFieldName,
            int keyFactoryId,
            int keyClassId,
            String valueFormat
    ) {
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " EXTERNAL NAME " + mapName + "("
                + keyFieldName + " INT EXTERNAL NAME \"__key.id\""
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + keyFactoryId + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + keyClassId + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + valueFormat + '\''
                + ")"
        );
    }
}

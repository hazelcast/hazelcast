/*
 * Copyright 2025 Hazelcast Inc.
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

import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// https://hazelcast.atlassian.net/browse/HZ-4026
@RunWith(HazelcastSerialClassRunner.class)
public class SqlPortableCompoundKeysTest extends SqlTestSupport {

    @BeforeClass
    public static void setUp() throws Exception {
        Config config = smallInstanceConfig();
        config.getSerializationConfig()
                .addPortableFactory(MyPortableFactory.ID, new MyPortableFactory());
        initialize(1, config);
    }

    public static class PortablePojoEmptyDefaultCtor implements Portable {
        public static final int ID = 1;

        protected Integer f0;
        protected Integer f1;
        protected Integer f2;

        public PortablePojoEmptyDefaultCtor() {
        }

        public PortablePojoEmptyDefaultCtor(Integer f0, Integer f1, Integer f2) {
            this.f0 = f0;
            this.f1 = f1;
            this.f2 = f2;
        }

        public Integer getF0() {
            return f0;
        }

        public void setF0(Integer f0) {
            this.f0 = f0;
        }

        public Integer getF1() {
            return f1;
        }

        public void setF1(Integer f1) {
            this.f1 = f1;
        }

        public Integer getF2() {
            return f2;
        }

        public void setF2(Integer f2) {
            this.f2 = f2;
        }

        @Override
        public int getFactoryId() {
            return MyPortableFactory.ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("f0", f0);
            writer.writeInt("f1", f1);
            writer.writeInt("f2", f2);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            f0 = reader.readInt("f0");
            f1 = reader.readInt("f1");
            f2 = reader.readInt("f2");
        }
    }

    public static class PortablePojoDefaultingDefaultCtor extends PortablePojoEmptyDefaultCtor {
        public static final int ID = 2;

        public PortablePojoDefaultingDefaultCtor() {
            f0 = 0;
            f1 = 0;
            f2 = 0;
        }

        public PortablePojoDefaultingDefaultCtor(Integer f0, Integer f1, Integer f2) {
            super(f0, f1, f2);
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            super.writePortable(writer);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            super.readPortable(reader);
        }
    }

    public static class MyPortableFactory implements PortableFactory {
        public static final int ID = 1338;

        @Override
        public Portable create(final int classId) {
            if (classId == PortablePojoEmptyDefaultCtor.ID) {
                return new PortablePojoEmptyDefaultCtor();
            }
            if (classId == PortablePojoDefaultingDefaultCtor.ID) {
                return new PortablePojoDefaultingDefaultCtor();
            }
            return null;
        }
    }

    /**
     * Test for fallback option in
     * {@link MetadataPortableResolver#resolveAndValidateFields(boolean, List, Map, InternalSerializationService)}.
     */
    @Test
    public void when_portableObjectHasEmptyDefaultCtor_then_fails() {
        String mapName = randomName();

        IMap<PortablePojoEmptyDefaultCtor, String> map = instance().getMap(mapName);

        assertThatThrownBy(() -> instance().getSql().execute(
                "CREATE OR REPLACE MAPPING " + mapName
                        + " TYPE " + IMapSqlConnector.TYPE_NAME + " "
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                        + ", '" + OPTION_KEY_FACTORY_ID + "'='" + MyPortableFactory.ID + '\''
                        + ", '" + OPTION_KEY_CLASS_ID + "'='" + PortablePojoEmptyDefaultCtor.ID + '\''
                        + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + 0 + '\''
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                        + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                        + ")"
        )).hasMessageContaining("Cannot create mapping for Portable type");
    }

    /**
     * Test for fallback option in
     * {@link MetadataPortableResolver#resolveAndValidateFields(boolean, List, Map, InternalSerializationService)}.
     */
    @Test
    public void when_portableObjectHasNonEmptyDefaultCtor_then_ok() {
        String mapName = randomName();

        IMap<PortablePojoEmptyDefaultCtor, String> map = instance().getMap(mapName);

        try (SqlResult result = instance().getSql().execute(
                "CREATE OR REPLACE MAPPING " + mapName
                        + " TYPE " + IMapSqlConnector.TYPE_NAME + " "
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                        + ", '" + OPTION_KEY_FACTORY_ID + "'='" + MyPortableFactory.ID + '\''
                        + ", '" + OPTION_KEY_CLASS_ID + "'='" + PortablePojoDefaultingDefaultCtor.ID + '\''
                        + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + 0 + '\''
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                        + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                        + ")"
        )) {
            assertThat(result.updateCount()).isEqualTo(0);
        }

        // Ensure there is no query runtime errors.
        assertRowsAnyOrder("SELECT __key, this FROM " + mapName, emptyList());
    }
}

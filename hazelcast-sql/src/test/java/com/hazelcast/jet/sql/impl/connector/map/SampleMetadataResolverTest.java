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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.VersionedPortable;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@SuppressWarnings("ConstantConditions")
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SampleMetadataResolverTest {

    private static final int PORTABLE_FACTORY_ID = 1;
    private static final int PORTABLE_CLASS_ID = 2;
    private static final int PORTABLE_CLASS_VERSION = 3;

    @Parameter
    public boolean key;

    @Parameters(name = "key:{0}")
    public static Collection<Boolean> parameters() {
        return asList(true, false);
    }

    @Test
    public void test_versionedPortable() {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_CLASS_ID, PORTABLE_CLASS_VERSION).build();
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().addClassDefinition(classDefinition).build();

        Metadata metadata = SampleMetadataResolver.resolve(ss, new VersionedPortableClass(), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, PORTABLE_FORMAT),
                entry(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(PORTABLE_FACTORY_ID)),
                entry(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(PORTABLE_CLASS_ID)),
                entry(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, String.valueOf(PORTABLE_CLASS_VERSION))
        );

        metadata = SampleMetadataResolver.resolve(ss, ss.toData(new VersionedPortableClass()), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, PORTABLE_FORMAT),
                entry(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(PORTABLE_FACTORY_ID)),
                entry(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(PORTABLE_CLASS_ID)),
                entry(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, String.valueOf(PORTABLE_CLASS_VERSION))
        );
    }

    @Test
    public void test_portable() {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_CLASS_ID, 0).build();
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().addClassDefinition(classDefinition).build();

        Metadata metadata = SampleMetadataResolver.resolve(ss, new PortableClass(), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, PORTABLE_FORMAT),
                entry(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(PORTABLE_FACTORY_ID)),
                entry(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(PORTABLE_CLASS_ID)),
                entry(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, "0")
        );

        metadata = SampleMetadataResolver.resolve(ss, ss.toData(new PortableClass()), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, PORTABLE_FORMAT),
                entry(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(PORTABLE_FACTORY_ID)),
                entry(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(PORTABLE_CLASS_ID)),
                entry(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, "0")
        );
    }

    @Test
    public void test_portableRecord() {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_CLASS_ID, PORTABLE_CLASS_VERSION).build();
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().addClassDefinition(classDefinition).build();

        Metadata metadata = SampleMetadataResolver.resolve(ss, new PortableGenericRecordBuilder(classDefinition).build(), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, PORTABLE_FORMAT),
                entry(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(PORTABLE_FACTORY_ID)),
                entry(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(PORTABLE_CLASS_ID)),
                entry(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, String.valueOf(PORTABLE_CLASS_VERSION))
        );

        metadata = SampleMetadataResolver.resolve(ss, ss.toData(new PortableGenericRecordBuilder(classDefinition).build()), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, PORTABLE_FORMAT),
                entry(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(PORTABLE_FACTORY_ID)),
                entry(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(PORTABLE_CLASS_ID)),
                entry(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, String.valueOf(PORTABLE_CLASS_VERSION))
        );
    }

    @Test
    public void test_compact() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(CompactClass.class, "type-name", new CompactClass.CompactClassSerializer());
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
                .setSchemaService(CompactTestUtil.createInMemorySchemaService())
                .setConfig(serializationConfig)
                .build();

        Metadata metadata = SampleMetadataResolver.resolve(ss, new CompactClass(1), key);
        assertThat(metadata.fields()).containsExactly(
                new MappingField("field", QueryDataType.INT, (key ? KEY : VALUE) + ".field")
        );
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, COMPACT_FORMAT),
                entry(key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME, "type-name")
        );

        metadata = SampleMetadataResolver.resolve(ss, ss.toData(new CompactClass(1)), key);
        assertThat(metadata.fields()).containsExactly(
                new MappingField("field", QueryDataType.INT, (key ? KEY : VALUE) + ".field")
        );
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, COMPACT_FORMAT),
                entry(key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME, "type-name")
        );
    }

    @Test
    public void test_compactRecord() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true);
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
                .setSchemaService(CompactTestUtil.createInMemorySchemaService())
                .setConfig(serializationConfig)
                .build();

        Metadata metadata = SampleMetadataResolver.resolve(ss, GenericRecordBuilder.compact("type-name").setInt32("field", 1).build(), key);
        assertThat(metadata.fields()).containsExactly(
                new MappingField("field", QueryDataType.INT, (key ? KEY : VALUE) + ".field")
        );
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, COMPACT_FORMAT),
                entry(key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME, "type-name")
        );

        metadata = SampleMetadataResolver.resolve(ss, ss.toData(GenericRecordBuilder.compact("type-name").setInt32("field", 1).build()), key);
        assertThat(metadata.fields()).containsExactly(
                new MappingField("field", QueryDataType.INT, (key ? KEY : VALUE) + ".field")
        );
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, COMPACT_FORMAT),
                entry(key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME, "type-name")
        );
    }

    @Test
    public void test_json() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        Metadata metadata = SampleMetadataResolver.resolve(ss, new HazelcastJsonValue("{}"), key);
        assertThat(metadata).isNull();

        metadata = SampleMetadataResolver.resolve(ss, ss.toData(new HazelcastJsonValue("{}")), key);
        assertThat(metadata).isNull();
    }

    @Test
    public void test_java() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        Metadata metadata = SampleMetadataResolver.resolve(ss, new Value(), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, JAVA_FORMAT),
                entry(key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS, Value.class.getName())
        );

        metadata = SampleMetadataResolver.resolve(ss, ss.toData(new Value()), key);
        assertThat(metadata.options()).containsExactly(
                entry(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, JAVA_FORMAT),
                entry(key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS, Value.class.getName())
        );
    }

    private static final class PortableClass implements Portable {

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) {
        }

        @Override
        public void readPortable(PortableReader reader) {
        }
    }

    private static final class VersionedPortableClass implements VersionedPortable {

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_CLASS_ID;
        }

        @Override
        public int getClassVersion() {
            return PORTABLE_CLASS_VERSION;
        }

        @Override
        public void writePortable(PortableWriter writer) {
        }

        @Override
        public void readPortable(PortableReader reader) {
        }
    }

    private static class CompactClass {

        public int field;

        private CompactClass(int field) {
            this.field = field;
        }

        private static class CompactClassSerializer implements CompactSerializer<CompactClass> {

            @Nonnull
            @Override
            public CompactClass read(@Nonnull CompactReader in) {
                return new CompactClass(in.readInt32("field"));
            }

            @Override
            public void write(@Nonnull CompactWriter out, @Nonnull CompactClass object) {
                out.writeInt32("field", object.field);
            }
        }
    }

    private static final class Value implements Serializable {
    }
}

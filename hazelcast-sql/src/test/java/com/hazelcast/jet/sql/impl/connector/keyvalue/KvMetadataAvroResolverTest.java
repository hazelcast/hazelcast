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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.jet.sql.impl.extract.AvroQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.AvroUpsertTargetDescriptor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class KvMetadataAvroResolverTest {

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveFields(boolean key, String prefix) {
        Stream<MappingField> fields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field", QueryDataType.INT, prefix + ".field")),
                emptyMap(),
                null
        );

        assertThat(fields).containsExactly(field("field", QueryDataType.INT, prefix + ".field"));
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void when_noKeyOrThisPrefixInExternalName_then_usesValue(boolean key) {
        KvMetadata metadata = INSTANCE.resolveMetadata(
                key,
                singletonList(field("field", QueryDataType.INT, "extField")),
                emptyMap(),
                null
        );
        assertThat(metadata.getFields()).containsExactly(
                key
                        ? new MapTableField[]{
                                new MapTableField("__key", QueryDataType.OBJECT, true, QueryPath.KEY_PATH)
                        }
                        : new MapTableField[]{
                                new MapTableField("field", QueryDataType.INT, false, new QueryPath("extField", false)),
                                new MapTableField("this", QueryDataType.OBJECT, true, QueryPath.VALUE_PATH)
                        });
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_duplicateExternalName_then_throws(boolean key, String prefix) {
        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                key,
                asList(
                        field("field1", QueryDataType.INT, prefix + ".field"),
                        field("field2", QueryDataType.VARCHAR, prefix + ".field")
                ),
                emptyMap(),
                null
        )).isInstanceOf(QueryException.class)
          .hasMessageMatching("Duplicate external name: (__key|this).field");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    @SuppressWarnings("checkstyle:LineLength")
    public void test_resolveMetadata(boolean key, String prefix) {
        KvMetadata metadata = INSTANCE.resolveMetadata(
                key,
                asList(
                        field("string", QueryDataType.VARCHAR, prefix + ".string"),
                        field("boolean", QueryDataType.BOOLEAN, prefix + ".boolean"),
                        field("byte", QueryDataType.TINYINT, prefix + ".byte"),
                        field("short", QueryDataType.SMALLINT, prefix + ".short"),
                        field("int", QueryDataType.INT, prefix + ".int"),
                        field("long", QueryDataType.BIGINT, prefix + ".long"),
                        field("float", QueryDataType.REAL, prefix + ".float"),
                        field("double", QueryDataType.DOUBLE, prefix + ".double"),
                        field("decimal", QueryDataType.DECIMAL, prefix + ".decimal"),
                        field("time", QueryDataType.TIME, prefix + ".time"),
                        field("date", QueryDataType.DATE, prefix + ".date"),
                        field("timestamp", QueryDataType.TIMESTAMP, prefix + ".timestamp"),
                        field("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, prefix + ".timestampTz"),
                        field("object", QueryDataType.OBJECT, prefix + ".object")
                ),
                emptyMap(),
                null
        );

        assertThat(metadata.getFields()).containsExactly(
                new MapTableField("string", QueryDataType.VARCHAR, false, QueryPath.create(prefix + ".string")),
                new MapTableField("boolean", QueryDataType.BOOLEAN, false, QueryPath.create(prefix + ".boolean")),
                new MapTableField("byte", QueryDataType.TINYINT, false, QueryPath.create(prefix + ".byte")),
                new MapTableField("short", QueryDataType.SMALLINT, false, QueryPath.create(prefix + ".short")),
                new MapTableField("int", QueryDataType.INT, false, QueryPath.create(prefix + ".int")),
                new MapTableField("long", QueryDataType.BIGINT, false, QueryPath.create(prefix + ".long")),
                new MapTableField("float", QueryDataType.REAL, false, QueryPath.create(prefix + ".float")),
                new MapTableField("double", QueryDataType.DOUBLE, false, QueryPath.create(prefix + ".double")),
                new MapTableField("decimal", QueryDataType.DECIMAL, false, QueryPath.create(prefix + ".decimal")),
                new MapTableField("time", QueryDataType.TIME, false, QueryPath.create(prefix + ".time")),
                new MapTableField("date", QueryDataType.DATE, false, QueryPath.create(prefix + ".date")),
                new MapTableField("timestamp", QueryDataType.TIMESTAMP, false, QueryPath.create(prefix + ".timestamp")),
                new MapTableField("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, false,
                        QueryPath.create(prefix + ".timestampTz")),
                new MapTableField("object", QueryDataType.OBJECT, false, QueryPath.create(prefix + ".object")),
                new MapTableField(prefix, QueryDataType.OBJECT, true, QueryPath.create(prefix))
        );
        assertThat(metadata.getQueryTargetDescriptor()).isEqualTo(AvroQueryTargetDescriptor.INSTANCE);
        assertThat(metadata.getUpsertTargetDescriptor()).isEqualToComparingFieldByField(
                new AvroUpsertTargetDescriptor(SchemaBuilder.record("jet.sql")
                        .fields()
                        .name("string").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                        .name("boolean").type().unionOf().nullType().and().booleanType().endUnion().nullDefault()
                        .name("byte").type().unionOf().nullType().and().intType().endUnion().nullDefault()
                        .name("short").type().unionOf().nullType().and().intType().endUnion().nullDefault()
                        .name("int").type().unionOf().nullType().and().intType().endUnion().nullDefault()
                        .name("long").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                        .name("float").type().unionOf().nullType().and().floatType().endUnion().nullDefault()
                        .name("double").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
                        .name("decimal").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                        .name("time").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                        .name("date").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                        .name("timestamp").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                        .name("timestampTz").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                        .name("object").type()
                                .unionOf().nullType()
                                .and().booleanType()
                                .and().intType()
                                .and().longType()
                                .and().floatType()
                                .and().doubleType()
                                .and().stringType()
                                .endUnion().nullDefault()
                        .endRecord()));
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }
}

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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AvroResolverTest {

    @Test
    public void test_resolveFields() {
        // given
        Schema schema = SchemaBuilder.record("name")
                .fields()
                .name("boolean").type().booleanType().noDefault()
                .name("int").type().intType().noDefault()
                .name("long").type().longType().noDefault()
                .name("float").type().floatType().noDefault()
                .name("double").type().doubleType().noDefault()
                .name("string").type().stringType().noDefault()
                .name("object").type().record("object").fields().endRecord().noDefault()
                .endRecord();

        // when
        List<MappingField> fields = AvroResolver.resolveFields(schema);

        // then
        assertThat(fields).hasSize(7);
        assertThat(fields.get(0)).isEqualTo(new MappingField("boolean", QueryDataType.BOOLEAN));
        assertThat(fields.get(1)).isEqualTo(new MappingField("int", QueryDataType.INT));
        assertThat(fields.get(2)).isEqualTo(new MappingField("long", QueryDataType.BIGINT));
        assertThat(fields.get(3)).isEqualTo(new MappingField("float", QueryDataType.REAL));
        assertThat(fields.get(4)).isEqualTo(new MappingField("double", QueryDataType.DOUBLE));
        assertThat(fields.get(5)).isEqualTo(new MappingField("string", QueryDataType.VARCHAR));
        assertThat(fields.get(6)).isEqualTo(new MappingField("object", QueryDataType.OBJECT));
    }
}

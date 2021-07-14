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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.sql.impl.connector.file.FileTable.FilePlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class FileTableTest {

    @SuppressWarnings({"unused", "checkstyle:LineLength"})
    private Object[] keys() {
        return new Object[]{
                new Object[]{"schema1", "name1", "field1", FileFormat.json(), "schema1", "name1", "field1", FileFormat.json(), true},
                new Object[]{"schema1", "name1", "field1", FileFormat.json(), "schema2", "name1", "field1", FileFormat.json(), false},
                new Object[]{"schema1", "name1", "field1", FileFormat.json(), "schema1", "name2", "field1", FileFormat.json(), false},
                new Object[]{"schema1", "name1", "field1", FileFormat.json(), "schema1", "name1", "field2", FileFormat.json(), false},
                new Object[]{"schema1", "name1", "field1", FileFormat.json(), "schema1", "name1", "field1", FileFormat.avro(), false},
        };
    }

    @Test
    @Parameters(method = "keys")
    @SuppressWarnings("checkstyle:ParameterNumber")
    public void test_objectKeyEquality(
            String schema1, String name1, String field1, FileFormat<?> format1,
            String schema2, String name2, String field2, FileFormat<?> format2,
            boolean expected
    ) {
        FilePlanObjectKey k1 = new FilePlanObjectKey(
                schema1,
                name1,
                ImmutableList.of(new TableField(field1, QueryDataType.INT, false)),
                new ProcessorMetaSupplierProvider(ImmutableMap.of("key", "value"), format1)
        );
        FilePlanObjectKey k2 = new FilePlanObjectKey(
                schema2,
                name2,
                singletonList(new TableField(field2, QueryDataType.INT, false)),
                new ProcessorMetaSupplierProvider(singletonMap("key", "value"), format2)
        );

        assertThat(k1.equals(k2)).isEqualTo(expected);
        assertThat(k1.hashCode() == k2.hashCode()).isEqualTo(expected);
    }
}

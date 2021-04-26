/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaTable.KafkaPlanObjectKey;
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
public class KafkaTableTest {

    @SuppressWarnings({"unused", "checkstyle:LineLength"})
    private Object[] keys() {
        return new Object[]{
                new Object[]{"schema1", "name1", "topic1", "field1", "value1", "schema1", "name1", "topic1", "field1", "value1", true},
                new Object[]{"schema1", "name1", "topic1", "field1", "value1", "schema2", "name1", "topic1", "field1", "value1", false},
                new Object[]{"schema1", "name1", "topic1", "field1", "value1", "schema1", "name2", "topic1", "field1", "value1", false},
                new Object[]{"schema1", "name1", "topic1", "field1", "value1", "schema1", "name1", "topic2", "field1", "value1", false},
                new Object[]{"schema1", "name1", "topic1", "field1", "value1", "schema1", "name1", "topic1", "field2", "value1", false},
                new Object[]{"schema1", "name1", "topic1", "field1", "value1", "schema1", "name1", "topic1", "field1", "value2", false},
        };
    }

    @Test
    @Parameters(method = "keys")
    @SuppressWarnings("checkstyle:ParameterNumber")
    public void test_objectKeyEquality(
            String schema1, String name1, String topic1, String field1, String value1,
            String schema2, String name2, String topic2, String field2, String value2,
            boolean expected
    ) {
        KafkaPlanObjectKey k1 = new KafkaPlanObjectKey(
                schema1,
                name1,
                topic1,
                ImmutableList.of(new TableField(field1, QueryDataType.INT, false)),
                ImmutableMap.of("key", value1)
        );
        KafkaPlanObjectKey k2 = new KafkaPlanObjectKey(
                schema2,
                name2,
                topic2,
                singletonList(new TableField(field2, QueryDataType.INT, false)),
                singletonMap("key", value2)
        );

        assertThat(k1.equals(k2)).isEqualTo(expected);
        assertThat(k1.hashCode() == k2.hashCode()).isEqualTo(expected);
    }
}

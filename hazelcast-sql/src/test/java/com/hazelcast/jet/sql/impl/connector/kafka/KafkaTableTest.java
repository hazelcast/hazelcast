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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.kafka.impl.StreamKafkaP;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaTable.KafkaPlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_PREFERRED_LOCAL_PARALLELISM;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                List.of(new TableField(field1, QueryDataType.INT, false)),
                Map.of("key", value1)
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

    @SuppressWarnings("unused")
    private Object[] preferredLocalParallelisms() {
        return new Object[]{
            new Object[]{"-2", -2, false},
            new Object[]{"-1", -1, false},
            new Object[]{"0", 0, false},
            new Object[]{"1", 1, false},
            new Object[]{"2", 2, false},
            new Object[]{"not-an-int", null, true},
            new Object[]{"3.14159", null, true},
        };
    }

    @Test
    @Parameters(method = "preferredLocalParallelisms")
    public void when_preferredLocalParallelism_isDefined_then_parseInt(String plp, Integer expected, boolean shouldThrow) {
        KafkaTable table = new KafkaTable(
                null, null, null, null, null, null, null,
                Map.of(OPTION_PREFERRED_LOCAL_PARALLELISM, plp),
                null, null, null, null, null
        );

        if (shouldThrow) {
            assertThatThrownBy(table::preferredLocalParallelism)
                    .isInstanceOf(NumberFormatException.class);
        } else {
            assertThat(table.preferredLocalParallelism()).isEqualTo(expected);
        }
    }

    @Test
    public void when_preferredLocalParallelism_isNotDefined_then_useDefault() {
        KafkaTable table = new KafkaTable(
                null, null, null, null, null, null, null,
                emptyMap(),
                null, null, null, null, null
        );

        assertThat(table.preferredLocalParallelism())
                .isEqualTo(StreamKafkaP.PREFERRED_LOCAL_PARALLELISM);
    }
}

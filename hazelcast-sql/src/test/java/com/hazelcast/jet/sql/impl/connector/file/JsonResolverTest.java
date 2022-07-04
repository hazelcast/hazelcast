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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonResolverTest {

    @Test
    public void test_resolveFields() {
        // given
        Map<String, Object> json = new LinkedHashMap<String, Object>() {
            {
                put("boolean", true);
                put("number", 1);
                put("string", "string");
                put("object", emptyMap());
                put("array", emptyList());
                put("nullValue", null);
                put("null", null);
            }
        };

        // when
        List<MappingField> fields = JsonResolver.resolveFields(json);

        // then
        assertThat(fields).hasSize(7);
        assertThat(fields.get(0)).isEqualTo(new MappingField("boolean", QueryDataType.BOOLEAN));
        assertThat(fields.get(1)).isEqualTo(new MappingField("number", QueryDataType.DOUBLE));
        assertThat(fields.get(2)).isEqualTo(new MappingField("string", QueryDataType.VARCHAR));
        assertThat(fields.get(3)).isEqualTo(new MappingField("object", QueryDataType.OBJECT));
        assertThat(fields.get(4)).isEqualTo(new MappingField("array", QueryDataType.OBJECT));
        assertThat(fields.get(5)).isEqualTo(new MappingField("nullValue", QueryDataType.OBJECT));
        assertThat(fields.get(6)).isEqualTo(new MappingField("null", QueryDataType.OBJECT));
    }
}

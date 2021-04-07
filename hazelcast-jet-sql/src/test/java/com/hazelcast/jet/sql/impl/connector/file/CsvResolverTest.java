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

package com.hazelcast.jet.sql.impl.connector.file;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvResolverTest {

    @Test
    public void test_resolveFields() {
        // given
        Set<String> headers = ImmutableSet.of("field1", "field2");

        // when
        List<MappingField> fields = CsvResolver.resolveFields(headers);

        // then
        assertThat(fields).hasSize(2);
        assertThat(fields.get(0)).isEqualTo(new MappingField("field1", QueryDataType.VARCHAR));
        assertThat(fields.get(1)).isEqualTo(new MappingField("field2", QueryDataType.VARCHAR));
    }
}

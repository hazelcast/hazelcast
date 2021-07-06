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

import com.google.common.collect.ImmutableSet;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

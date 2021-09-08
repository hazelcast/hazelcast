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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class MetadataTest {

    @Test
    public void test_merge() {
        Metadata first = new Metadata(
                asList(
                        new MappingField("field1", QueryDataType.INT, "__key.field1"),
                        new MappingField("field2", QueryDataType.INT, "__key.field2")
                ),
                ImmutableMap.of("key1", "1", "key2", "2")
        );
        Metadata second = new Metadata(
                asList(
                        new MappingField("field2", QueryDataType.VARCHAR, "this.field2"),
                        new MappingField("field3", QueryDataType.VARCHAR, "this.field3")
                ),
                ImmutableMap.of("key2", "two", "key3", "three")
        );

        Metadata merged = first.merge(second);
        assertThat(merged).isEqualToComparingFieldByField(new Metadata(
                asList(
                        new MappingField("field1", QueryDataType.INT, "__key.field1"),
                        new MappingField("field2", QueryDataType.INT, "__key.field2"),
                        new MappingField("field3", QueryDataType.VARCHAR, "this.field3")
                ),
                ImmutableMap.of("key1", "1", "key2", "2", "key3", "three")
        ));
    }
}

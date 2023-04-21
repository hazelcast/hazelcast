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

package com.hazelcast.mapstore;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MappingHelperTest {

    @Test
    public void externalName_simple() {
        String input = "my_table";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"my_table\"");
    }

    @Test
    public void externalName_with_dot() {
        String input = "custom_schema.my_table";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with_2dots() {
        String input = "catalog.custom_schema.my_table";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"catalog\".\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_withQuotes() {
        String input = "custom_schema.\"my_table\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with2Quotes() {
        String input = "\"custom_schema\".\"my_table\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with3Quotes() {
        String input = "\"catalog\".\"custom_schema\".\"my_table\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"catalog\".\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with_quotes_and_dots() {
        String input = "custom_schema.\"table.with_dot\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"table.with_dot\"");
    }

    @Test
    public void externalName_with_backticks_and_dots() {
        String input = "custom_schema.`table.with_dot`";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"table.with_dot\"");
    }
}

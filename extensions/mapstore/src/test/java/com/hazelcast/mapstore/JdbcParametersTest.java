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

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcParametersTest {

    @Test
    public void testShiftParametersForUpdateForPosition0() {
        JdbcParameters jDBCParameters = new JdbcParameters();
        jDBCParameters.setParams(new Object[]{1, 2, 3});

        jDBCParameters.shiftIdParameterToEnd();

        Object[] params = jDBCParameters.getParams();

        assertThat(params).isEqualTo(new Object[]{2, 3, 1});
    }

    @Test
    public void testShiftParametersForUpdateForPosition1() {
        JdbcParameters jDBCParameters = new JdbcParameters();
        jDBCParameters.setParams(new Object[]{1, 2, 3});
        jDBCParameters.setIdPos(1);

        jDBCParameters.shiftIdParameterToEnd();

        Object[] params = jDBCParameters.getParams();

        assertThat(params).isEqualTo(new Object[]{1, 3, 2});
    }

    @Test
    public void testToJDBCParameters() {
        List<SqlColumnMetadata> columnMetadata = Arrays.asList(
                new SqlColumnMetadata("id", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("name", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("address", SqlColumnType.VARCHAR, true)
        );

        // Create GenericRecord containing id column, the id column should be used as JDBC parameter
        GenericRecord genericRecord = GenericRecordBuilder.compact("person")
                                                          .setInt32("id", 1)
                                                          .setString("name", "name_value")
                                                          .setString("address", "address_value")
                                                          .build();

        Integer key = 10;
        JdbcParameters jdbcParameters = JdbcParameters.convert(key, genericRecord, columnMetadata, "id", false);

        Object[] expected = {10, "name_value", "address_value"};
        assertThat(jdbcParameters.getParams()).isEqualTo(expected);
    }
}

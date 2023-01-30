package com.hazelcast.mapstore;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GenericRecordUtilsTest {

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
        JDBCParameters jdbcParameters = GenericRecordUtils.toJDBCParameters(key, genericRecord,
                columnMetadata,
                "id");

        Object[] expected = {10, "name_value", "address_value"};
        assertThat(jdbcParameters.getParams()).isEqualTo(expected);
    }
}

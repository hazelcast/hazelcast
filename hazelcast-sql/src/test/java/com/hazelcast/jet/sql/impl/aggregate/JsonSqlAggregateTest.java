package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import junitparams.JUnitParamsRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;

@RunWith(JUnitParamsRunner.class)
public class JsonSqlAggregateTest extends SqlTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_json_arr_agg() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{null, "1"},
                new String[]{"Alice", "1"},
                new String[]{null, "1"}
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY NAME) FROM " + name + "",
                asList(
                        new Row("[Alice, Bob, null]")
                )
        );
    }

    private static String createTable(String[]... values) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(VARCHAR, INTEGER),
                asList(values)
        );
        return name;
    }
}

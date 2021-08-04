package com.hazelcast.sql.impl.expression;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO: merge into main suite
public class ExtendedCastFunctionIntegrationTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_varcharLiteralCastAsJson_jsonIsReturned() {
        for (final SqlRow row : instance().getSql().execute("SELECT CAST('[1,2,3]' AS JSON)")) {
            System.out.println(row);
        }
    }

    @Test
    public void when_varcharColumnCastAsJson_jsonIsReturned() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        for (final SqlRow row : instance().getSql().execute("SELECT CAST(this AS JSON) FROM test")) {
            System.out.println(row);
        }
    }
}

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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractFunctionIntegrationTest extends ExpressionTestSupport {

    @Test
    public void test() {
        put(1);

        check(sql("MILLENNIUM", "'0001-04-23'"), 1.0);
        check(sql("MILLENNIUM", "'2010-11-10'"), 3.0);

        check(sql("CENTURY", "'0001-04-23'"), 1.0);
        check(sql("CENTURY", "'2010-11-10'"), 21.0);

        check(sql("DECADE", "'0001-04-23'"), 0.0);
        check(sql("DECADE", "'2010-11-10'"), 201.0);

        check(sql("YEAR", "'2006-01-01'"), 2006.0);
        check(sql("YEAR", "'2006-01-02'"), 2006.0);
        check(sql("YEAR", "'2012-12-30'"), 2012.0);
        check(sql("YEAR", "'2012-12-31'"), 2012.0);

        check(sql("ISOYEAR", "'2006-01-01'"), 2005.0);
        check(sql("ISOYEAR", "'2006-01-02'"), 2006.0);
        check(sql("ISOYEAR", "'2012-12-30'"), 2012.0);
        check(sql("ISOYEAR", "'2012-12-31'"), 2013.0);

        check(sql("QUARTER", "'2006-01-01'"), 1.0);
        check(sql("QUARTER", "'2006-04-01'"), 2.0);
        check(sql("QUARTER", "'2006-07-01'"), 3.0);
        check(sql("QUARTER", "'2006-10-01'"), 4.0);

        check(sql("MONTH", "'2006-01-01'"), 1.0);
        check(sql("MONTH", "'2006-01-01 12:30:33'"), 1.0);
        check(sql("MONTH", "'2006-12-01'"), 12.0);
        check(sql("MONTH", "'2006-12-01 12:30:33'"), 12.0);

        check(sql("WEEK", "'2005-01-01'"), 53.0);
        check(sql("WEEK", "'2006-01-01'"), 52.0);
        check(sql("WEEK", "'2012-12-31'"), 1.0);


        check(sql("DOW", "'2021-04-19'"), 1.0);
        check(sql("DOW", "'2021-04-18'"), 0.0);
        check(sql("DOW", "'2021-04-17'"), 6.0);

        check(sql("ISODOW", "'2021-04-19'"), 1.0);
        check(sql("ISODOW", "'2021-04-18'"), 7.0);
        check(sql("ISODOW", "'2021-04-17'"), 6.0);

        check(sql("DOY", "'2001-01-01'"), 1.0);
        check(sql("DOY", "'2001-12-31'"), 365.0);
        check(sql("DOY", "'2100-12-31'"), 365.0);
        check(sql("DOY", "'2000-12-31'"), 366.0);
        check(sql("DOY", "'2004-12-31'"), 366.0);
        check(sql("DOY", "'2001-02-16'"), 47.0);

        check(sql("DAY", "'0001-04-23'"), 23.0);
        check(sql("DAY", "'2010-11-10'"), 10.0);

        check(sql("HOUR", "'2001-02-16 00:00:30'"), 0.0);
        check(sql("HOUR", "'2001-02-16 20:38:40'"), 20.0);
        check(sql("HOUR", "'2001-02-16'"), 0.0);

        check(sql("MINUTE", "'2001-02-16 00:00:30'"), 0.0);
        check(sql("MINUTE", "'2001-02-16 20:38:40'"), 38.0);
        check(sql("MINUTE", "'2001-02-16'"), 0.0);

        check(sql("MILLISECOND", "'0001-01-01'"), 0.0);
        check(sql("MILLISECOND", "'0001-01-01 10:30:30.54'"), 30_540.0);

        check(sql("MICROSECOND", "'0001-01-01'"), 0.0);
        check(sql("MICROSECOND", "'0001-01-01 10:30:30.54'"), 30_540_000.0);

        check(sql("EPOCH", "'2001-02-16 20:38:40'"), 982_355_920.0);
        check(sql("EPOCH", "'2001-02-16 20:38:40.123'"), 982_355_920.123);
    }


    private <T> void check(String sql, T expectedResult, Object ...parameters) {
        List<SqlRow> rows = execute(member, sql, parameters);
        SqlRow row = rows.get(0);

        SqlColumnType typeOfReceived = row.getMetadata().getColumn(0).getType();

        assertEquals(SqlColumnType.DOUBLE, typeOfReceived);
        assertEquals(expectedResult, row.getObject(0));
    }

    private String sql(Object field, Object timestamp) {
        return String.format("SELECT EXTRACT(%s FROM TIMESTAMP %s) FROM map", field, timestamp);
    }
}

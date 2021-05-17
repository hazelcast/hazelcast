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

package com.hazelcast.jet.sql;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlMemoryManagement extends SqlTestSupport {

    private static final int MAX_PROCESSOR_ACCUMULATED_RECORDS = 2;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig().getInstanceConfig()
                .setCooperativeThreadCount(1)
                .setMaxProcessorAccumulatedRecords(MAX_PROCESSOR_ACCUMULATED_RECORDS);

        initialize(1, config);
        sqlService = instance().getSql();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileGrouping_then_throws() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                singletonList("name"),
                singletonList(QueryDataTypeFamily.VARCHAR),
                asList(new String[]{"Alice"}, new String[]{"Bob"}, new String[]{"Joe"})
        );

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " + name + " GROUP BY name").iterator().next())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }
}

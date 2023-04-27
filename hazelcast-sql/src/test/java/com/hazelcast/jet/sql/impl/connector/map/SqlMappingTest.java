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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlMappingTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void testExternalNameShouldNotHaveMoreComponents() {
        String query = "CREATE MAPPING map EXTERNAL NAME \"schema1\".\"mapName\" ("
                + "__key INT,"
                + "this VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ")";

        assertThatThrownBy(() -> sqlService.execute(query))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid external name \"schema1\".\"mapName\"");
    }

    @Test
    public void testSqlCatalogCannotBeMapped() {
        testInternalMapCannotBeMapped(JetServiceBackend.SQL_CATALOG_MAP_NAME);
    }

    @Test
    public void testJetImapsCannotBeMapped() {
        testInternalMapCannotBeMapped(JobRepository.JOB_RECORDS_MAP_NAME);
        testInternalMapCannotBeMapped(JobRepository.JOB_EXECUTION_RECORDS_MAP_NAME);
        testInternalMapCannotBeMapped(JobRepository.JOB_RESULTS_MAP_NAME);
        testInternalMapCannotBeMapped(JobRepository.JOB_METRICS_MAP_NAME);
    }

    private void testInternalMapCannotBeMapped(String name) {
        String query = "CREATE MAPPING map EXTERNAL NAME \"" + name + "\" "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Object.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + Object.class.getName() + '\''
                + ")";

        assertThatThrownBy(() -> sqlService.execute(query))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Mapping of internal IMaps is not allowed");
    }

}

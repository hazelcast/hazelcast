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

package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.sql.HazelcastSqlException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlMappingIT extends MongoSqlIT {

    @Test
    public void testExternalNameShouldNotHaveMoreComponents() {
        String query = "CREATE MAPPING col EXTERNAL NAME \"" + databaseName + "\".\"collection\".\"aa\" ("
                + "__key INT,"
                + "this VARCHAR"
                + ") TYPE " + MongoSqlConnector.TYPE_NAME + ' '
                + options();

        assertThatThrownBy(() -> sqlService.execute(query))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid external name \"" + databaseName + "\".\"collection\".\"aa\"");
    }

}

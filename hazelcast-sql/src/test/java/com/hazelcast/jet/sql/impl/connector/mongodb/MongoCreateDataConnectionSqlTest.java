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

import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MongoCreateDataConnectionSqlTest extends MongoSqlTest {

    @Test
    public void test() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA CONNECTION " + dlName + " TYPE Mongo SHARED " + options());


        DataConnection dataConnection = getNodeEngineImpl(
                instance()).getDataConnectionService().getAndRetainDataConnection(dlName, MongoDataConnection.class);

        assertThat(dataConnection).isNotNull();
        assertThat(dataConnection.getConfig().getType()).isEqualTo("Mongo");
    }
}

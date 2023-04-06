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

import com.hazelcast.datalink.DataLink;
import com.hazelcast.jet.mongodb.datalink.MongoDataLink;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MongoCreateDataLinkSqlTest extends MongoSqlTest {

    @Test
    public void test() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName + " TYPE MongoDB SHARED " + options());


        DataLink dataLink = getNodeEngineImpl(
                instance()).getDataLinkService().getAndRetainDataLink(dlName, MongoDataLink.class);

        assertThat(dataLink).isNotNull();
        assertThat(dataLink.getConfig().getType()).isEqualTo("MongoDB");
    }
}

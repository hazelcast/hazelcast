/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.serialization.compact.record;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

public class SqlJava16RecordTest extends SqlTestSupport {

    @Test
    public void test() {
        Config config = smallInstanceConfig();
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        HazelcastInstance inst = createHazelcastInstance(config);
        SqlService sqlService = inst.getSql();

        inst.getMap("m").put(1, new Person("foo", 42));
        sqlService.execute("CREATE OR REPLACE MAPPING m(__key int, age INT, name VARCHAR) TYPE imap\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='int'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + "'\n"
                + ", 'valueCompactTypeName'='" + Person.class.getName() + "'\n"
                + ")"
        );
        sqlService.execute("insert into m values (2, 43, 'foo43')");
        for (SqlRow r : sqlService.execute("select * from m")) {
            System.out.println(r);
        }
    }

    public record Person(String name, int age) { }
}

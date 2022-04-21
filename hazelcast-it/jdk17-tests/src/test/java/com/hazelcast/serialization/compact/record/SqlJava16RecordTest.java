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
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.client.SqlClientService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.junit.Assert.assertEquals;

public class SqlJava16RecordTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        initializeWithClient(1, config, null);
    }

    @Test
    public void test_withColumnList2() {
        instance().getMap("m").put(1, new Person("foo", 42));
        SqlService sqlService = instance().getSql();
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

    @Test
    public void test_withColumnList() {
        instance().getMap("m").put(1, new Person("foo", 42));
        SqlService sqlService = instance().getSql();
        sqlService.execute("CREATE OR REPLACE MAPPING m(__key int, age INT, name VARCHAR) TYPE imap\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='int'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + "'\n"
                + ", 'valueCompactTypeName'='s" + Person.class.getName() + "'\n"
                + ")"
        );
        sqlService.execute("insert into m values (2, 43, 'foo43')");
        for (SqlRow r : sqlService.execute("select * from m")) {
            System.out.println(r);
        }
    }

    @Test
    public void test_withoutColumnList() {
        instance().getMap("m").put(1, new Person("foo", 42));
        SqlService sqlService = instance().getSql();
        sqlService.execute("CREATE OR REPLACE MAPPING m TYPE imap\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='int'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + "'\n"
                + ", 'valueCompactTypeName'='s" + Person.class.getName() + "'\n"
                + ")"
        );
        sqlService.execute("insert into m values (2, 43, 'foo43')");
        for (SqlRow r : sqlService.execute("select * from m")) {
            System.out.println(r);
        }
    }

    @Test
    public void test_ddlCreation() throws Exception {
        instance().getMap("m").put(1, new Person("foo", 42));
        SqlClientService sqlService = (SqlClientService) client().getSql();
        String mappingCommand = sqlService.mappingDdl(instance().getCluster().getLocalMember(), "m").get();
        assertEquals(
                "CREATE MAPPING \"m\" (\n" +
                "  \"__key\" INTEGER EXTERNAL NAME \"__key\",\n" +
                "  \"age\" INTEGER EXTERNAL NAME \"this.age\",\n" +
                "  \"name\" VARCHAR EXTERNAL NAME \"this.name\"\n" +
                ")\n" +
                "TYPE IMap\n" +
                "OPTIONS (\n" +
                "  'keyFormat' = 'java',\n" +
                "  'keyJavaClass' = 'java.lang.Integer',\n" +
                "  'valueFormat' = 'compact',\n" +
                "  'valueCompactTypeName' = 'com.hazelcast.serialization.compact.record.SqlJava16RecordTest$Person'\n" +
                ")", mappingCommand);
    }

    public record Person(String name, int age) { }

    // for records that implement Serializable, Compact isn't used.
    public record SerializablePerson(String name, int age) implements Serializable { }
}

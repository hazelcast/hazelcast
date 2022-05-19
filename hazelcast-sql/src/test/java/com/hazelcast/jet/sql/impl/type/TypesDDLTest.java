/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static java.lang.String.format;

@RunWith(HazelcastSerialClassRunner.class)
public class TypesDDLTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void test_createTypeIsNotDuplicatedByDefault() {
        instance().getSql().execute(format("CREATE TYPE FirstType OPTIONS ('typeJavaClass'='%s')", FirstType.class.getName()));
        assertThrows(HazelcastException.class, () -> instance().getSql()
                .execute(format("CREATE TYPE FirstType OPTIONS ('typeJavaClass'='%s')", SecondType.class.getName())));
    }

    @Test
    public void test_replaceType() {
        execute(format("CREATE TYPE FirstType OPTIONS ('typeJavaClass'='%s')", FirstType.class.getName()));
        execute(format("CREATE OR REPLACE TYPE FirstType OPTIONS ('typeJavaClass'='%s')", SecondType.class.getName()));
    }

    @Test
    public void test_createIfNotExists() {
        execute(format("CREATE TYPE FirstType OPTIONS ('typeJavaClass'='%s')", FirstType.class.getName()));
        execute(format("CREATE TYPE IF NOT EXISTS FirstType OPTIONS ('typeJavaClass'='%s')", SecondType.class.getName()));
    }

    void execute(String sql) {
        instance().getSql().execute(sql);
    }

    public static class FirstType implements Serializable {
        private String name;

        public FirstType() { }

        public FirstType(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class SecondType implements Serializable {
        private String name;

        public SecondType() { }

        public SecondType(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }
}

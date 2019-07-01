/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package integration;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JDBC {

    static void s1() {
        String DB_CONNECTION_URL = "";
        //tag::s1[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.jdbc(
            () -> DriverManager.getConnection(DB_CONNECTION_URL),
            (con, parallelism, index) -> {
                PreparedStatement stmt = con.prepareStatement("SELECT * FROM PERSON WHERE MOD(id, ?) = ?)");
                stmt.setInt(1, parallelism);
                stmt.setInt(2, index);
                return stmt.executeQuery();
            },
            resultSet ->
                new Person(resultSet.getInt(1), resultSet.getString(2))
        )).drainTo(Sinks.logger());
        //end::s1[]
    }

    static void s2() {
        String DB_CONNECTION_URL = "";
        //tag::s2[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.jdbc(
            DB_CONNECTION_URL,
            "select * from PERSON",
            resultSet ->
                    new Person(resultSet.getInt(1), resultSet.getString(2))
        )).drainTo(Sinks.logger());
        //end::s2[]
    }

    static void s3() {
        String DB_CONNECTION_URL = "";
        //tag::s3[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Person>list("inputList"))
         .drainTo(Sinks.jdbc(
                 "REPLACE INTO PERSON (id, name) values(?, ?)",
                 DB_CONNECTION_URL,
                 (stmt, item) -> {
                     stmt.setInt(1, item.id);
                     stmt.setString(2, item.name);
                 }));
        //end::s3[]
    }

    static class Person {

        private final int id;
        private final String name;

        Person(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

}

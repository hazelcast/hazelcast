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

package com.hazelcast.sql;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.sql.pojos.Person;
import org.apache.calcite.linq4j.Enumerable;

import java.util.Arrays;
import java.util.Scanner;

public class Demo {

    public static void main(String[] args) {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("persons");
        for (int i = 0; i < 10; ++i) {
            map.put(i, new Person(i));
        }

        HazelcastSql hazelcastSql = HazelcastSql.createFor(instance);

        String query = "select height from persons where age >= 5 order by name";
//        String query = "select age, height from persons where age >= 5";

        try {
//            System.out.println(hazelcastSql.explainLogical(query));
//            System.out.println(hazelcastSql.explain(query));

            Enumerable<Object> result = hazelcastSql.query(query);

            for (Object object : result) {
                System.out.println(object instanceof Object[] ? Arrays.deepToString((Object[]) object) : object);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}

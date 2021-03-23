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

package com.hazelcast.sql;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import java.io.Serializable;

public class Testt {

    @Test
    public void test() {
        HazelcastInstance inst = Hazelcast.newHazelcastInstance();
        inst.getMap("m").put(1, new Foo());
        for (SqlRow r : inst.getSql().execute("select * from m")) {
            System.out.println(r);
        }
    }

    private static class Foo implements Serializable {
//        public String getField() {
//            return "1";
//        }
        public String getfield() {
            return "2";
        }
    }
}

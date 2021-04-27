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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import domainclasses.Employee;

import java.io.IOException;
import java.util.Map;

//TODO sancar delete
public class XSerialization2API {


    public static void main(String[] args) throws InterruptedException {
        zeroConfig();
        alreadySerializable();
        objectNotInOurClassPath();
        notInOurClasspath_Write();
        notInOutClassPath_Executor();
        fullConfigInteroperability();
        interoperableButNotInOurClasPath();
        schemaEvolution();
        schemaEvolutionNotInOurClassPath();
    }

    private static void schemaEvolutionNotInOurClassPath() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");

        GenericRecord genericRecord = GenericRecordBuilder.compact("employee")
                .setString("name", "John")
                .setInt("age", 20)
                .setString("surname", "Smith").build();
        map.put(1, genericRecord);
        GenericRecord employee = (GenericRecord) map.get(1);

        String name = employee.getString("name");
        int age = employee.getInt("age");
        String surname;
        if (employee.hasField("surname") && employee.getFieldType("surname").equals(FieldType.UTF)) {
            surname = employee.getString("surname");
        } else {
            surname = "NOT AVAILABLE";
        }
    }

    private static void schemaEvolution() {
        //Example for schema evolution
        Config config = new Config();
        CompactSerializationConfig compactSerializationConfig = config.getSerializationConfig().getCompactSerializationConfig();
        compactSerializationConfig.register(Employee.class, "employee", new CompactSerializer<Employee>() {
            @Override
            public Employee read(CompactReader in) throws IOException {
                String name = in.getString("name");
                int age = in.getInt("age");
                String surname = in.getString("surname", "NOT AVAILABLE");
                return new Employee(name, age, surname);
            }

            @Override
            public void write(CompactWriter out, Employee object) throws IOException {
                out.writeString("name", object.getName());
                out.writeInt("age", object.getAge());
                out.writeString("surname", object.getSurname());
            }
        });

        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, new Employee());
        Employee employee = (Employee) map.get(1);
    }

    private static void interoperableButNotInOurClasPath() {
        //When it needs to be interoperable between java and non-java
        //And the caller does not have the class in its classpath
        //No config is necessary
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        GenericRecord genericRecord = GenericRecordBuilder.compact("employee")
                .setString("name", "myName")
                .setInt("age", 20).build();

        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, genericRecord);
        GenericRecord response = (GenericRecord) map.get(1);
    }

    private static void fullConfigInteroperability() {
        //When it needs to be interoperable between java and non-java
        Config config = new Config();
        CompactSerializationConfig compactSerializationConfig = config.getSerializationConfig().getCompactSerializationConfig();
        compactSerializationConfig.register(Employee.class);
        //it is advised to give explicit type name. Default class name includes package name
        //it is advised to give explicit serializer. This way it is simpler to match types of the fields.
        //When user does not give explicit serializer, we can try to create a reflective serializer but not straightforward
        //in most of the languages.
        // 1. C++ does not have this.
        // 2. Node js does not have different types for integer, long etc. Not clear in which type we should write a number.
        //......
        compactSerializationConfig.register(Employee.class, "employee", new CompactSerializer<Employee>() {
            @Override
            public Employee read(CompactReader in) throws IOException {
                String name = in.getString("name");
                int age = in.getInt("age");
                return new Employee(name, age);
            }

            @Override
            public void write(CompactWriter out, Employee object) throws IOException {
                out.writeString("name", object.getName());
                out.writeInt("age", object.getAge());
            }
        });

        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, new Employee());
        Employee employee = (Employee) map.get(1);
    }

    private static void notInOutClassPath_Executor() {
        //Lets say that this map has `Person` object and the object is not in our classpath
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");

        map.executeOnKey(1, new EntryProcessor<Object, Object, Object>() {
            @Override
            public Object process(Map.Entry<Object, Object> entry) {
                GenericRecord record = (GenericRecord) entry.getKey();
                GenericRecord genericRecord = record.cloneWithBuilder()
                        .setInt("age", 21).build();
                entry.setValue(genericRecord);
                return true;
            }
        });
    }

    private static void objectNotInOurClassPath() {
        //Lets say that this map has `Person` object and the object is not in our classpath
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");
        GenericRecord person = (GenericRecord) map.get(1);
    }

    private static void alreadySerializable() {
        //Employee class in our classpath.
        // and it should be configured explicitly in config
        Config config = new Config();
        CompactSerializationConfig compactSerializationConfig = config.getSerializationConfig().getCompactSerializationConfig();
        compactSerializationConfig.register(Employee.class);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, new Employee());
        Employee employee = (Employee) map.get(1);
    }

    private static void zeroConfig() {
        //Employee class in our classpath.

        //In order Employee be serialized via new serializer,
        // 1) it should not extend Portable,IdentifiedDataSerializable,Serializable.
        //    And user should not configure a global serializer!!!

        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, new Employee("aName", 20));
        Employee employee = (Employee) map.get(1);
    }

    private static void notInOurClasspath_Write() {
        //Lets say that this map has `Person` object and the object is not in our classpath
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");

        GenericRecord genericRecord = GenericRecordBuilder.compact("employee")
                .setString("name", "John")
                .setInt("age", 20).build();
        map.put(1, genericRecord);
        GenericRecord person = (GenericRecord) map.get(1);
    }
}

/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecord;

import java.io.Serializable;

/**
 * DTO for a user.
 */
public class User implements Serializable, SpecificRecord {

    private static final Schema SCHEMA = SchemaBuilder
            .record(User.class.getSimpleName())
            .namespace(User.class.getPackage().getName())
            .fields()
                .name("username").type().stringType().noDefault()
                .name("password").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .name("status").type().booleanType().noDefault()
            .endRecord();

    private String username;
    private String password;
    private int age;
    private boolean status;

    public User() {
    }

    public User(String username, String password, int age, boolean status) {
        this.username = username;
        this.password = password;
        this.age = age;
        this.status = status;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                username = v.toString();
                break;
            case 1:
                password = v.toString();
                break;
            case 2:
                age = (Integer) v;
                break;
            case 3:
                status = (Boolean) v;
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return username;
            case 1:
                return password;
            case 2:
                return age;
            case 3:
                return status;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }

    @Override
    public String toString() {
        return "avro.model.User{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", age=" + age +
                ", status=" + status +
                '}';
    }
}

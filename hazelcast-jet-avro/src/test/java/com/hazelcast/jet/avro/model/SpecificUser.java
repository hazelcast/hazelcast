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

package com.hazelcast.jet.avro.model;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecord;

public class SpecificUser extends User implements SpecificRecord {

    public static Schema SCHEMA$;

    static {
        SCHEMA$ = SchemaBuilder.record(SpecificUser.class.getSimpleName())
                               .namespace(SpecificUser.class.getPackage().getName())
                               .fields()
                               .name("username").type().stringType().noDefault()
                               .name("password").type().stringType().noDefault()
                               .endRecord();
    }

    public SpecificUser() {
    }

    public SpecificUser(String username, String password) {
        super(username, password);
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                setUsername(v.toString());
                break;
            case 1:
                setPassword(v.toString());
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return getUsername();
            case 1:
                return getPassword();
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }
}

/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate.local;


import com.hazelcast.hibernate.serialization.HibernateDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * An invalidation messages
 */
public class Invalidation implements IdentifiedDataSerializable {

    private Object key;
    private Object version;

    public Invalidation() {
    }

    public Invalidation(final Object key, final Object version) {
        this.key = key;
        this.version = version;
    }

    public Object getKey() {
        return key;
    }

    public Object getVersion() {
        return version;
    }

    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(version);
    }

    public void readData(final ObjectDataInput in) throws IOException {
        key = in.readObject();
        version = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return HibernateDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HibernateDataSerializerHook.INVALIDATION;
    }

    @Override
    public String toString() {
        return "Invalidation{key=" + key + ", version=" + version + '}';
    }
}

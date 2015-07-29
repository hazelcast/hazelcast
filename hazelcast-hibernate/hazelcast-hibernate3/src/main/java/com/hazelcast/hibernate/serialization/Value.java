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

package com.hazelcast.hibernate.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Comparator;

/**
 * A value within a region cache
 */
public class Value extends Expirable {

    private long timestamp;
    private Object value;

    public Value() {
    }

    public Value(Object version, long timestamp, Object value) {
        super(version);
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean isReplaceableBy(long txTimestamp, Object newVersion, Comparator versionComparator) {
        return version == null
                ? timestamp <= txTimestamp
                : versionComparator.compare(version, newVersion) < 0;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public Object getValue(long txTimestamp) {
        return timestamp <= txTimestamp ? value : null;
    }

    @Override
    public boolean matches(ExpiryMarker lock) {
        return false;
    }

    @Override
    public ExpiryMarker markForExpiration(long timeout, String nextMarkerId) {
        return new ExpiryMarker(version, timeout, nextMarkerId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        timestamp = in.readLong();
        value = in.readObject();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(timestamp);
        out.writeObject(value);
    }

    @Override
    public int getFactoryId() {
        return HibernateDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HibernateDataSerializerHook.VALUE;
    }

}

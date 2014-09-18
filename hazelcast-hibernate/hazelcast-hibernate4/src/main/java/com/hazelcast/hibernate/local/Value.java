/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import org.hibernate.cache.spi.access.SoftLock;

/**
 * A value wrapper with version, lock support and creationTime
 */
public class Value {

    private final Object version;

    private final Object value;

    private final SoftLock lock;

    private final long creationTime;

    public Value(final Object version, final Object value, final long creationTime) {
        this.version = version;
        this.value = value;
        this.creationTime = creationTime;
        this.lock = null;
    }

    public Value(final Object version, final Object value, final SoftLock lock, final long creationTime) {
        this.version = version;
        this.value = value;
        this.lock = lock;
        this.creationTime = creationTime;
    }

    public Object getValue() {
        return value;
    }

    public Object getVersion() {
        return version;
    }

    public SoftLock getLock() {
        return lock;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Value createLockedValue(SoftLock lock) {
        return new Value(version, value, lock, creationTime);
    }

    public Value createUnlockedValue() {
        return new Value(version, value, null, creationTime);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Value value1 = (Value) o;

        if (value != null ? !value.equals(value1.value) : value1.value != null) {
            return false;
        }
        if (version != null ? !version.equals(value1.version) : value1.version != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = version != null ? version.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Value");
        sb.append("{value=").append(value);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }

}


/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.record;

class ObjectRecordWithStats extends AbstractRecord<Object> {
    private volatile Object value;

    ObjectRecordWithStats() {
    }

    ObjectRecordWithStats(Object value) {
        setValue(value);
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public long getCost() {
        return 0L;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        ObjectRecordWithStats that = (ObjectRecordWithStats) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ObjectRecordWithStats{"
                + "value=" + value
                + ", " + super.toString()
                + "} ";
    }
}

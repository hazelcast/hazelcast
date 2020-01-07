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

package com.hazelcast.map.impl.record;

public class ObjectRecord extends AbstractRecord<Object> implements Record<Object> {
    private volatile Object value;

    ObjectRecord() {
        super();
    }

    ObjectRecord(Object value) {
        super();
        this.value = value;
    }

    // as there is no easy way to calculate the size of
    // Object cost is not implemented for ObjectRecord
    @Override
    public long getCost() {
        return 0L;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setValue(Object o) {
        value = o;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        ObjectRecord that = (ObjectRecord) o;
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
        return "ObjectRecord{"
                + "value=" + value
                + ", " + super.toString()
                + "} ";
    }
}

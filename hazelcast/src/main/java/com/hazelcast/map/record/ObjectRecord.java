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

package com.hazelcast.map.record;

import com.hazelcast.nio.serialization.Data;

public final class ObjectRecord extends AbstractRecord<Object> implements Record<Object> {

    private Object value;

    public ObjectRecord(Data keyData, Object value, boolean statisticsEnabled) {
        super(keyData, statisticsEnabled);
        this.value = value;
    }

    public ObjectRecord() {
    }

    // as there is no easy way to calculate the size of Object cost is not implemented for ObjectRecord
    @Override
    public long getCost() {
        return 0;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object o) {
        value = o;
    }

    public void invalidate() {
        value = null;
    }
}

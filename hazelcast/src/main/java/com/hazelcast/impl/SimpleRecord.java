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

package com.hazelcast.impl;

import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toObject;

public class SimpleRecord extends AbstractSimpleRecord implements Record {

    Data value;

    public SimpleRecord(int blockId, CMap cmap, long id, Data key, Data value) {
        super(blockId, cmap, id, key);
        this.value = value;
    }

    public Record copy() {
        return new SimpleRecord(blockId, cmap, id, key, value);
    }

    public Object getValue() {
        return toObject(getValueData());
    }

    public Data getValueData() {
        return value;
    }

    public void setValueData(Data value) {
        this.value = value;
    }

    public long getCost() {
        return key.size() + value.size() + 30;
    }

    public boolean hasValueData() {
        return value != null;
    }

    public Object setValue(Object value) {
        return null;
    }

    public int valueCount() {
        return 1;
    }

    public void invalidate() {
        value = null;
    }
}

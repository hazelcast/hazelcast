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

package com.hazelcast.multimap;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.EventFilter;

import java.io.IOException;

/**
 * @author ali 1/9/13
 */
public class MultiMapEventFilter implements EventFilter, DataSerializable {

    boolean includeValue;

    Data key;

    public MultiMapEventFilter() {
    }

    public MultiMapEventFilter(boolean includeValue, Data key) {
        this.includeValue = includeValue;
        this.key = key;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public Data getKey() {
        return key;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(includeValue);
        IOUtil.writeNullableData(out, key);
    }

    public void readData(ObjectDataInput in) throws IOException {
        includeValue = in.readBoolean();
        key = IOUtil.readNullableData(in);
    }

    public boolean eval(Object arg) {
        return false;
    }
}

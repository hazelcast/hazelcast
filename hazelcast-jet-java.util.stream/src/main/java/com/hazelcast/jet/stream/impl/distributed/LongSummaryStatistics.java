/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.distributed;

import com.hazelcast.jet.stream.impl.StreamUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

import static com.hazelcast.jet.stream.impl.StreamUtil.setPrivateField;

public class LongSummaryStatistics extends java.util.LongSummaryStatistics implements DataSerializable {

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(this.getCount());
        out.writeLong(this.getSum());
        out.writeLong(this.getMin());
        out.writeLong(this.getMax());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        try {
            Class<?> clazz = java.util.LongSummaryStatistics.class;
            setPrivateField(this, clazz, "count", in.readLong());
            setPrivateField(this, clazz, "sum", in.readLong());
            setPrivateField(this, clazz, "min", in.readLong());
            setPrivateField(this, clazz, "max", in.readLong());
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw StreamUtil.reThrow(e);
        }
    }
}

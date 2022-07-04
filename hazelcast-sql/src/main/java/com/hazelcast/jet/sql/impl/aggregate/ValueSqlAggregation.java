/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * Special-case aggregation that aggregates multiple instances of the same
 * value. The result is the value. Asserts that no non-equal value is
 * accumulated.
 */
@NotThreadSafe
public class ValueSqlAggregation implements SqlAggregation {

    private Object value;

    @Override
    public void accumulate(Object value) {
        this.value = value;
    }

    @Override
    public void combine(SqlAggregation other0) {
        ValueSqlAggregation other = (ValueSqlAggregation) other0;
        this.value = other.value;
    }

    @Override
    public Object collect() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, ((SerializationServiceSupport) out).getSerializationService().toData(value));
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = IOUtil.readData(in);
    }
}

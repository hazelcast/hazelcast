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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashSet;
import java.util.Set;

@NotThreadSafe
class DistinctSqlAggregation implements SqlAggregation {

    private final Set<Object> values;

    private final SqlAggregation delegate;

    DistinctSqlAggregation(SqlAggregation delegate) {
        this.values = new HashSet<>();

        this.delegate = delegate;
    }

    @Override
    public void accumulate(Object value) {
        if (value != null && !values.add(value)) {
            return;
        }

        delegate.accumulate(value);
    }

    @Override
    public void combine(SqlAggregation other0) {
        DistinctSqlAggregation other = (DistinctSqlAggregation) other0;

        for (Object object: other.values) {
            accumulate(object);
        }
    }

    @Override
    public Object collect() {
        return delegate.collect();
    }

    @Override
    public void writeData(ObjectDataOutput out) {
        // this class is never serialized - we use it only in single-stage aggregations
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public void readData(ObjectDataInput in) {
        // this class is never serialized - we use it only in single-stage aggregations
        throw new UnsupportedOperationException("Should not be called");
    }
}

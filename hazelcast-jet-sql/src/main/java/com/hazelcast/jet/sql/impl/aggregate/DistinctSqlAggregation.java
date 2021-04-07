/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
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

        delegate.combine(other.delegate);
    }

    @Override
    public Object collect() {
        return delegate.collect();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // this class is never serialized - we use it only in single-stage aggregations
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        // this class is never serialized - we use it only in single-stage aggregations
        throw new UnsupportedOperationException("Should not be called");
    }
}

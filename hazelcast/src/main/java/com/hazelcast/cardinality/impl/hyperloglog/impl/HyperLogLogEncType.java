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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public enum HyperLogLogEncType {

    SPARSE {
        @Override
        public HyperLogLog build(int p) {
            return new SparseHyperLogLog(p);
        }
    },
    DENSE {
        @Override
        public HyperLogLog build(int p) {
            return new DenseHyperLogLog(p);
        }
    },
    COMPO {
        @Override
        public HyperLogLog build(int p) {
            return new CompositeHyperLogLogStore(p);
        }
    };

    public abstract HyperLogLog build(final int p);

    private static class CompositeHyperLogLogStore implements HyperLogLog, IHyperLogLogCompositeContext {

        private HyperLogLog hll;

        CompositeHyperLogLogStore(final int p) {
            hll = new SparseHyperLogLog(this, p);
        }

        @Override
        public long estimate() {
            return hll.estimate();
        }

        @Override
        public boolean aggregate(long hash) {
            return hll.aggregate(hash);
        }

        @Override
        public boolean aggregateAll(long[] hashes) {
            return hll.aggregateAll(hashes);
        }

        public void setHll(HyperLogLog hll) {
            this.hll = hll;
        }

        @Override
        public HyperLogLogEncType getEncodingType() {
            return COMPO;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            hll.writeData(out);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            hll.readData(in);
        }
    }
}


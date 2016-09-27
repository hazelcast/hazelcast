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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;

public class BatchNearCacheInvalidation extends Invalidation {

    private List<SingleNearCacheInvalidation> invalidations;

    public BatchNearCacheInvalidation() {
    }

    public BatchNearCacheInvalidation(int size, String mapName) {
        this(new ArrayList<SingleNearCacheInvalidation>(size), mapName);
    }

    public BatchNearCacheInvalidation(List<SingleNearCacheInvalidation> invalidations, String mapName) {
        super(mapName);

        this.invalidations = checkNotNull(invalidations, "invalidations cannot be null");
    }

    @Override
    public void consumedBy(InvalidationHandler invalidationHandler) {
        invalidationHandler.handle(this);
    }

    public void add(SingleNearCacheInvalidation invalidation) {
        invalidations.add(invalidation);
    }

    public List<SingleNearCacheInvalidation> getInvalidations() {
        return invalidations;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(invalidations.size());
        for (SingleNearCacheInvalidation singleNearCacheInvalidation : invalidations) {
            singleNearCacheInvalidation.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        int size = in.readInt();
        if (size != 0) {
            List<SingleNearCacheInvalidation> invalidations = new ArrayList<SingleNearCacheInvalidation>(size);
            for (int i = 0; i < size; i++) {
                SingleNearCacheInvalidation invalidation = new SingleNearCacheInvalidation();
                invalidation.readData(in);

                invalidations.add(invalidation);
            }
            this.invalidations = invalidations;
        } else {
            this.invalidations = emptyList();
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        for (SingleNearCacheInvalidation invalidation : invalidations) {
            str.append(invalidation.toString());
        }
        return str.toString();
    }
}

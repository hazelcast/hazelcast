/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class BatchNearCacheInvalidation extends Invalidation {

    private List<Invalidation> invalidations = emptyList();

    public BatchNearCacheInvalidation() {
    }

    public BatchNearCacheInvalidation(String dataStructureName, List<Invalidation> invalidations) {
        super(dataStructureName);
        this.invalidations = invalidations;
    }

    public List<Invalidation> getInvalidations() {
        return invalidations;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(invalidations.size());
        for (Invalidation invalidation : invalidations) {
            out.writeObject(invalidation);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        int size = in.readInt();
        if (size != 0) {
            List<Invalidation> invalidations = new ArrayList<Invalidation>(size);
            for (int i = 0; i < size; i++) {
                Invalidation invalidation = in.readObject();
                invalidations.add(invalidation);
            }
            this.invalidations = invalidations;
        }
    }

    @Override
    public String toString() {
        return "BatchNearCacheInvalidation{"
                + "dataStructureName=" + getName()
                + ", invalidation-count-in-this-batch=" + invalidations.size()
                + '}';
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.NEAR_CACHE_BATCH_INVALIDATION;
    }
}

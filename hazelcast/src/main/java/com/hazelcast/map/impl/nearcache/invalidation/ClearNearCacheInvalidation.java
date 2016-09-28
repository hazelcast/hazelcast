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

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents a clear invalidation event.
 */
public class ClearNearCacheInvalidation extends Invalidation {

    private String sourceUuid;

    public ClearNearCacheInvalidation() {
    }

    public ClearNearCacheInvalidation(String mapName, String sourceUuid) {
        super(mapName);

        this.sourceUuid = checkNotNull(sourceUuid, "sourceUuid cannot be null");
    }

    @Override
    public String getSourceUuid() {
        return sourceUuid;
    }

    @Override
    public void consumedBy(InvalidationHandler invalidationHandler) {
        invalidationHandler.handle(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeUTF(sourceUuid);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        sourceUuid = in.readUTF();
    }

    @Override
    public String toString() {
        return "ClearNearCacheInvalidation{"
                + "mapName='" + mapName + '\''
                + ", sourceUuid='" + sourceUuid + '\''
                + '}';
    }
}

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

package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkHasText;

/**
 * Operation factory for {@link MadePublishableOperation}.
 *
 * @see MadePublishableOperation
 */
public class MadePublishableOperationFactory implements OperationFactory {

    private String mapName;
    private String cacheId;

    public MadePublishableOperationFactory() {
    }

    public MadePublishableOperationFactory(String mapName, String cacheId) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");

        this.cacheId = cacheId;
        this.mapName = mapName;
    }

    @Override
    public Operation createOperation() {
        return new MadePublishableOperation(mapName, cacheId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeString(cacheId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        cacheId = in.readString();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MADE_PUBLISHABLE_FACTORY;
    }
}


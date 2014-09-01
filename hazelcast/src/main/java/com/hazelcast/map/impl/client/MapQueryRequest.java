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

package com.hazelcast.map.impl.client;

import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.IterationType;
import java.io.IOException;

public final class MapQueryRequest extends AbstractMapQueryRequest {

    private Predicate predicate;

    public MapQueryRequest() {
    }

    public MapQueryRequest(String name, Predicate predicate, IterationType iterationType) {
        super(name, iterationType);
        this.predicate = predicate;
    }

    @Override
    protected Predicate getPredicate() {
        return predicate;
    }

    public int getClassId() {
        return MapPortableHook.QUERY;
    }

    protected void writePortableInner(PortableWriter writer) throws IOException {
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(predicate);
    }

    protected void readPortableInner(PortableReader reader) throws IOException {
        final ObjectDataInput in = reader.getRawDataInput();
        predicate = in.readObject();
    }

    @Override
    public String getMethodName() {
        if (iterationType == IterationType.KEY) {
            return "keySet";
        } else if (iterationType == IterationType.VALUE) {
            return "values";
        } else if (iterationType == IterationType.ENTRY) {
            return "entrySet";
        }
        throw new IllegalArgumentException("IterationType[" + iterationType + "] is unknown!!!");
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{predicate};
    }
}

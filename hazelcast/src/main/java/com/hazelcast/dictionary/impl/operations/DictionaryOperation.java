/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl.operations;

import com.hazelcast.dictionary.impl.DictionaryContainer;
import com.hazelcast.dictionary.impl.DictionaryDataSerializerHook;
import com.hazelcast.dictionary.impl.DictionaryPartition;
import com.hazelcast.dictionary.impl.DictionaryService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class DictionaryOperation
        extends Operation
        implements IdentifiedDataSerializable, NamedOperation {

    protected DictionaryService dataSeriesService;
    protected DictionaryContainer container;
    protected DictionaryPartition partition;
    protected String name;

    public DictionaryOperation() {
    }

    public DictionaryOperation(String name) {
        this.name = name;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        dataSeriesService = getService();
        container = dataSeriesService.getDictionaryContainer(name);
        partition = container.getPartition(getPartitionId());
    }

    @Override
    public String getServiceName() {
        return DictionaryService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return DictionaryDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }
}

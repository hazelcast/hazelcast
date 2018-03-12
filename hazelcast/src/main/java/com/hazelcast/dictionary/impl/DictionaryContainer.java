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

package com.hazelcast.dictionary.impl;

import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Constructor;

public class DictionaryContainer {
    private final DictionaryConfig config;
    private final NodeEngineImpl nodeEngine;
    private final Partition[] partitions;
    private final EntryModel entryModel;
    private final Compiler compiler;

    public DictionaryContainer(DictionaryConfig dictionaryConfig, NodeEngineImpl nodeEngine, Compiler compiler) {
        this.config = dictionaryConfig;
        this.nodeEngine = nodeEngine;
        this.partitions = new Partition[nodeEngine.getPartitionService().getPartitionCount()];
        this.entryModel = new EntryModel(dictionaryConfig);
        this.compiler = compiler;

        EntryEncoder entryEncoder = newEntryEncoder();

        System.out.println("DictionaryContainer " + dictionaryConfig.getName() + " created");
        for (int k = 0; k < partitions.length; k++) {
            this.partitions[k] = new Partition(nodeEngine, dictionaryConfig, entryModel, entryEncoder);
        }
    }

    private EntryEncoder newEntryEncoder() {
        EntryEncoderCodegen codegen = new EntryEncoderCodegen(entryModel);
        codegen.generate();
        Class<EntryEncoder> encoderClazz = compiler.compile(codegen.className(), codegen.getCode());
        try {
            Constructor<EntryEncoder> constructor = encoderClazz.getConstructor(EntryModel.class);
            return constructor.newInstance(entryModel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Partition getPartition(int partitionId) {
        return partitions[partitionId];
    }
}

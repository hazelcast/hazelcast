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
import com.hazelcast.dictionary.impl.type.EntryType;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Constructor;

/**
 * A container for a single {@link com.hazelcast.dictionary.Dictionary}.
 */
public class DictionaryContainer {
    private final DictionaryPartition[] partitions;
    private final EntryType entryType;
    private final Compiler compiler;

    public DictionaryContainer(DictionaryConfig dictionaryConfig, NodeEngineImpl nodeEngine, Compiler compiler) {
        this.partitions = new DictionaryPartition[nodeEngine.getPartitionService().getPartitionCount()];
        this.entryType = new EntryType(dictionaryConfig);
        this.compiler = compiler;

        EntryEncoder entryEncoder = newEntryEncoder();

        System.out.println("DictionaryContainer " + dictionaryConfig.getName() + " created");
        for (int k = 0; k < partitions.length; k++) {
            this.partitions[k] = new DictionaryPartition(nodeEngine, dictionaryConfig, entryType, entryEncoder);
        }
    }

    private EntryEncoder newEntryEncoder() {
        EntryEncoderCodegen codegen = new EntryEncoderCodegen(entryType);
        codegen.generate();
        Class<EntryEncoder> encoderClazz = compiler.compile(codegen.className(), codegen.toCode());
        try {
            Constructor<EntryEncoder> constructor = encoderClazz.getConstructor(EntryType.class);

            return constructor.newInstance(entryType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public DictionaryPartition getPartition(int partitionId) {
        return partitions[partitionId];
    }
}

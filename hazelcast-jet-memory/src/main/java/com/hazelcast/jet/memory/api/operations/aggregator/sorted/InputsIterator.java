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

package com.hazelcast.jet.memory.api.operations.aggregator.sorted;

import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueReader;

public interface InputsIterator {
    long keySize(int inputId);

    long keyAddress(int inputId);

    long valueSize(int inputId);

    long valueAddress(int inputId);

    long recordAddress(int inputId);

    long recordsCount(int inputId);

    boolean nextSlot(int inputId);

    boolean nextSource(int inputId);

    boolean nextRecord(int inputId);

    int partitionId(int inputId);

    int getSourceId(int inputId);

    long getHashCode(int inputId);

    MemoryBlock getMemoryBlock(int inputId);

    int getInputsCount();

    void setInputs(SpillingKeyValueReader diskInput,
                   MemoryBlockChain memoryInput);
}

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

package com.hazelcast.jet.io.api.serialization;

import com.hazelcast.internal.memory.MemoryManager;

/**
 * This is factory interface to create JetDataInput instance
 * It lets us to de-serialize data directly from  memory managed by some MemoryManager.
 */
public interface JetInputFactory {

    /**
     * @param memoryManager - memory manager to be used for de-serialization;
     * @param service       - instance of serialization service;
     * @param useBigEndian  - defines if big-endian would be used;
     * @return JetDataInput object to read de-serialized data
     */
    JetDataInput createInput(MemoryManager memoryManager,
                             JetSerializationService service,
                             boolean useBigEndian);
}

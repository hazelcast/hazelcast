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

package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Event data contract which is sent to subscriber side.
 */
@BinaryInterface
public interface QueryCacheEventData extends Sequenced, EventData {

    Object getKey();

    Object getValue();

    Data getDataKey();

    Data getDataNewValue();

    Data getDataOldValue();

    long getCreationTime();

    void setSerializationService(SerializationService serializationService);
}

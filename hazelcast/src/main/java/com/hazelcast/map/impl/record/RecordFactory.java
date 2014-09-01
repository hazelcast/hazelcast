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

package com.hazelcast.map.impl.record;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;

/**
 * TODO need a better name than RecordFactory!
 *
 * @param <T> the type of object which is going to be created.
 */
public interface RecordFactory<T> {

    Record<T> newRecord(Data key, Object value);

    void setValue(Record<T> record, Object value);

    boolean isEquals(Object value1, Object value2);

    InMemoryFormat getStorageFormat();
}

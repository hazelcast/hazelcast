/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.type.QueryDataType;

public class GenericQueryTarget implements QueryTarget, GenericTargetAccessor {

    private final InternalSerializationService serializationService;
    private final Extractors extractors;
    private final boolean key;

    private Object rawTarget;
    private Object target;

    public GenericQueryTarget(InternalSerializationService serializationService, Extractors extractors, boolean key) {
        this.serializationService = serializationService;
        this.extractors = extractors;
        this.key = key;
    }

    @Override
    public void setTarget(Object target) {
        this.rawTarget = target;
        this.target = null;
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        if (path == null) {
            return new GenericTargetExtractor(key, this, type);
        } else {
            return new GenericFieldExtractor(key, this, type, extractors, path);
        }
    }

    @Override
    public Object getTarget() {
        if (target == null) {
            target = rawTarget instanceof Data ? serializationService.toObject(rawTarget) : rawTarget;
        }

        return target;
    }

    public boolean isKey() {
        return key;
    }
}

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

package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.DefaultValueCollector;

final class ExtractorGetter extends Getter {

    private final ValueExtractor extractor;
    private final Object arguments;
    private final InternalSerializationService serializationService;

    ExtractorGetter(InternalSerializationService serializationService, ValueExtractor extractor, Object arguments) {
        super(null);
        this.extractor = extractor;
        this.arguments = arguments;
        this.serializationService = serializationService;
    }

    @Override
    @SuppressWarnings("unchecked")
    Object getValue(Object target) throws Exception {
        Object extractionTarget = target;
        // This part will be improved in 3.7 to avoid extra allocation
        DefaultValueCollector collector = new DefaultValueCollector();
        if (target instanceof Data) {
            InternalGenericRecord record = serializationService.readAsInternalGenericRecord((Data) target);
            extractionTarget = new GenericRecordQueryReader(record);
        }
        extractor.extract(extractionTarget, arguments, collector);
        return collector.getResult();
    }

    @Override
    Class getReturnType() {
        throw new UnsupportedOperationException();
    }

    @Override
    boolean isCacheable() {
        return true;
    }

}

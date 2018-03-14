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

package com.hazelcast.client.test;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class IdentifiedFactory implements DataSerializableFactory {
    public static final int FACTORY_ID = 66;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == IdentifiedEntryProcessor.CLASS_ID) {
            return new IdentifiedEntryProcessor();
        }
        if (typeId == CustomComparator.CLASS_ID) {
            return new CustomComparator();
        }
        if (typeId == DistortInvalidationMetadataEntryProcessor.CLASS_ID) {
            return new DistortInvalidationMetadataEntryProcessor();
        }
        if (typeId == PrefixFilter.CLASS_ID) {
            return new PrefixFilter();
        }
        return null;
    }
}

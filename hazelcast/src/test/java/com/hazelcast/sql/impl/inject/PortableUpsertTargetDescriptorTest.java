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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PortableUpsertTargetDescriptorTest {

    @Test
    public void testSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        PortableUpsertTargetDescriptor descriptor = serializationService.toObject(
                serializationService.toData(
                        new PortableUpsertTargetDescriptor(1, 2, 3)
                )
        );

        assertEquals(descriptor.getFactoryId(), 1);
        assertEquals(descriptor.getClassId(), 2);
        assertEquals(descriptor.getClassVersion(), 3);
    }
}
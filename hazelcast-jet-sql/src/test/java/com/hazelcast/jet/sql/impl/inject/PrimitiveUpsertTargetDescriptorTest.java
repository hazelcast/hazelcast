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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PrimitiveUpsertTargetDescriptorTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_create() {
        PrimitiveUpsertTargetDescriptor descriptor = PrimitiveUpsertTargetDescriptor.INSTANCE;

        // when
        UpsertTarget target = descriptor.create(SERIALIZATION_SERVICE);

        // then
        assertThat(target).isInstanceOf(PrimitiveUpsertTarget.class);
    }

    @Test
    public void test_serialization() {
        PrimitiveUpsertTargetDescriptor original = PrimitiveUpsertTargetDescriptor.INSTANCE;

        // when
        PrimitiveUpsertTargetDescriptor serialized =
                SERIALIZATION_SERVICE.toObject(SERIALIZATION_SERVICE.toData(original));

        // then
        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}

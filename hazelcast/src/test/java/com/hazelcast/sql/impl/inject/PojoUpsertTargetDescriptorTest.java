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

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static java.util.Collections.emptyMap;

public class PojoUpsertTargetDescriptorTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    public void testCreate() {
        UpsertTargetDescriptor descriptor = SERIALIZATION_SERVICE.toObject(
                SERIALIZATION_SERVICE.toData(
                        new PojoUpsertTargetDescriptor(getClass().getName(), emptyMap())
                )
        );

        UpsertTarget upsertTarget = descriptor.create(SERIALIZATION_SERVICE);

        assertInstanceOf(PojoUpsertTarget.class, upsertTarget);
    }
}
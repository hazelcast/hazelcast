/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PortableUpsertTargetDescriptorTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_create() {
        PortableUpsertTargetDescriptor descriptor =
                new PortableUpsertTargetDescriptor(new ClassDefinitionBuilder(1, 2, 3).build());

        // when
        UpsertTarget target = descriptor.create(mock());

        // then
        assertThat(target).isInstanceOf(PortableUpsertTarget.class);
    }

    @Test
    public void test_serialization() {
        PortableUpsertTargetDescriptor original = new PortableUpsertTargetDescriptor(
                new ClassDefinitionBuilder(1, 2, 3)
                        .addIntField("int")
                        .addPortableField("object", new ClassDefinitionBuilder(4, 5, 6).build())
                        .build()
        );

        // when
        PortableUpsertTargetDescriptor serialized =
                SERIALIZATION_SERVICE.toObject(SERIALIZATION_SERVICE.toData(original));

        // then
        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}

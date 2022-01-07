/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.SchemaWriter;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactUpsertTargetDescriptorTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_create() {
        CompactUpsertTargetDescriptor descriptor =
                new CompactUpsertTargetDescriptor(new SchemaWriter("test").build());

        // when
        UpsertTarget target = descriptor.create(SERIALIZATION_SERVICE);

        // then
        assertThat(target).isInstanceOf(CompactUpsertTarget.class);
    }

    @Test
    public void test_serialization() {
        SchemaWriter schemaWriter = new SchemaWriter("test");
        schemaWriter.addField(new FieldDescriptor("int", FieldKind.INT32));
        schemaWriter.addField(new FieldDescriptor("long", FieldKind.INT64));
        CompactUpsertTargetDescriptor original = new CompactUpsertTargetDescriptor(schemaWriter.build());

        // when
        CompactUpsertTargetDescriptor serialized =
                SERIALIZATION_SERVICE.toObject(SERIALIZATION_SERVICE.toData(original));

        // then
        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}

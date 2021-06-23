/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KvProjectorTest {

    @Test
    public void test_project() {
        KvProjector projector = new KvProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.INT},
                new MultiplyingTarget(),
                new MultiplyingTarget()
        );

        Entry<Object, Object> entry = projector.project(new Object[]{1, 2});

        assertThat(entry.getKey()).isEqualTo(2);
        assertThat(entry.getValue()).isEqualTo(4);
    }

    @Test
    public void test_supplierSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        KvProjector.Supplier original = KvProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.VARCHAR},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE
        );

        KvProjector.Supplier serialized = serializationService.toObject(serializationService.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }

    private static final class MultiplyingTarget implements UpsertTarget {

        private Object value;

        private MultiplyingTarget() {
            value = -1;
        }

        @Override
        public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
            return value -> this.value = value;
        }

        @Override
        public void init() {
            value = null;
        }

        @Override
        public Object conclude() {
            return (int) value * 2;
        }
    }
}

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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.JetTestSupport.TEST_SS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KvProjectorTest {

    @Test
    public void test_project() {
        KvProjector projector = new KvProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.INT},
                new MultiplyingTarget(),
                new MultiplyingTarget(),
                false
        );

        Entry<Object, Object> entry = projector.project(new JetSqlRow(TEST_SS, new Object[]{1, 2}));

        assertThat(entry.getKey()).isEqualTo(2);
        assertThat(entry.getValue()).isEqualTo(4);
    }

    @Test
    public void test_projectAllowNulls() {
        KvProjector projector = new KvProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.INT},
                new NullTarget(),
                new NullTarget(),
                false
        );

        Entry<Object, Object> entry = projector.project(new JetSqlRow(TEST_SS, new Object[]{1, 2}));

        assertThat(entry.getKey()).isNull();
        assertThat(entry.getValue()).isNull();
    }

    @Test
    public void test_projectKeyNullNotAllowed() {
        KvProjector projector = new KvProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.INT},
                new NullTarget(),
                new MultiplyingTarget(),
                true
        );

        assertThatThrownBy(() -> projector.project(new JetSqlRow(TEST_SS, new Object[]{1, 2})))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot write NULL to '__key' field");
    }

    @Test
    public void test_projectValueNullNotAllowed() {
        KvProjector projector = new KvProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.INT},
                new MultiplyingTarget(),
                new NullTarget(),
                true
        );

        assertThatThrownBy(() -> projector.project(new JetSqlRow(TEST_SS, new Object[]{1, 2})))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot write NULL to 'this' field");
    }

    @Test
    public void test_supplierSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        KvProjector.Supplier original = KvProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.VARCHAR},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                true
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

    private static final class NullTarget implements UpsertTarget {

        @Override
        public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
            return value -> {
            };
        }

        @Override
        public void init() {
        }

        @Override
        public Object conclude() {
            return null;
        }
    }
}

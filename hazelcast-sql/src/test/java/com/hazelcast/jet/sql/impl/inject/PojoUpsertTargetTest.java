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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PojoUpsertTargetTest {

    @Test
    public void test_set() {
        UpsertTarget target = new PojoUpsertTarget(
                Pojo.class.getName(),
                ImmutableMap.of(
                        "intField", int.class.getName(),
                        "longField", long.class.getName(),
                        "stringField", String.class.getName()
                )
        );
        UpsertInjector intFieldInjector = target.createInjector("intField", QueryDataType.INT);
        UpsertInjector longFieldInjector = target.createInjector("longField", QueryDataType.BIGINT);
        UpsertInjector stringFieldInjector = target.createInjector("stringField", QueryDataType.VARCHAR);

        target.init();
        intFieldInjector.set(1);
        longFieldInjector.set(2L);
        stringFieldInjector.set("3");
        Object pojo = target.conclude();

        assertThat(pojo).isEqualTo(new Pojo(1, 2L, "3"));
    }

    @Test
    public void when_injectNullValueWithPrimitiveField_then_throws() {
        UpsertTarget target = new PojoUpsertTarget(
                Pojo.class.getName(),
                ImmutableMap.of("intField", int.class.getName())
        );
        UpsertInjector injector = target.createInjector("intField", QueryDataType.INT);

        target.init();
        assertThatThrownBy(() -> injector.set(null))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot set NULL to a primitive field");
    }

    @Test
    public void when_injectNullValueWithPrimitiveSetter_then_throws() {
        UpsertTarget target = new PojoUpsertTarget(
                Pojo.class.getName(),
                ImmutableMap.of("longField", long.class.getName())
        );
        UpsertInjector injector = target.createInjector("longField", QueryDataType.BIGINT);

        target.init();
        assertThatThrownBy(() -> injector.set(null))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot pass NULL to a method with a primitive argument");
    }

    @Test
    public void when_injectNonExistingPropertyValue_then_throws() {
        UpsertTarget target = new PojoUpsertTarget(
                Object.class.getName(),
                ImmutableMap.of("field", int.class.getName())
        );
        UpsertInjector injector = target.createInjector("field", QueryDataType.INT);

        target.init();
        assertThatThrownBy(() -> injector.set(1))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot set property \"field\" to class java.lang.Object: " +
                        "no set-method or public field available");
    }

    @Test
    public void when_injectNonExistingPropertyNullValue_then_succeeds() {
        UpsertTarget target = new PojoUpsertTarget(
                Object.class.getName(),
                ImmutableMap.of("field", int.class.getName())
        );
        UpsertInjector injector = target.createInjector("field", QueryDataType.INT);

        target.init();
        injector.set(null);
        Object pojo = target.conclude();

        assertThat(pojo).isNotNull();
    }

    @SuppressWarnings("unused")
    private static final class Pojo {

        public int intField;
        private long longField;
        private String stringField;

        Pojo() {
        }

        private Pojo(int intField, long longField, String stringField) {
            this.intField = intField;
            this.longField = longField;
            this.stringField = stringField;
        }

        public long getLongField() {
            return longField;
        }

        public void setLongField(long longField) {
            this.longField = longField;
        }

        public String getStringField() {
            return stringField;
        }

        public void setStringField(String stringField) {
            this.stringField = stringField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Pojo pojo = (Pojo) o;
            return intField == pojo.intField &&
                    longField == pojo.longField &&
                    Objects.equals(stringField, pojo.stringField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intField, longField, stringField);
        }
    }
}

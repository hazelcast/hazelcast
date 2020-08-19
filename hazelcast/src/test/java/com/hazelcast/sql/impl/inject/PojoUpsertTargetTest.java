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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PojoUpsertTargetTest {

    @Test
    public void testInject() {
        UpsertTarget target = new PojoUpsertTarget(Pojo.class.getName(), ImmutableMap.of("field1", int.class.getName()));

        UpsertInjector field1Injector = target.createInjector("field1");
        UpsertInjector field2Injector = target.createInjector("field2");

        target.init();
        field1Injector.set(1);
        field2Injector.set("value1");
        Object pojo1 = target.conclude();
        assertEquals(new Pojo(1, "value1"), pojo1);

        target.init();
        field1Injector.set(2);
        field2Injector.set("value2");
        Object pojo2 = target.conclude();
        assertEquals(new Pojo(2, "value2"), pojo2);
    }

    @Test
    public void testInjectNullIntoNonExistingField() {
        UpsertTarget target = new PojoUpsertTarget(Object.class.getName(), emptyMap());

        UpsertInjector injector = target.createInjector("nonExistingField");

        target.init();
        injector.set(null);
        Object pojo = target.conclude();
        assertNotNull(pojo);
    }

    @SuppressWarnings("unused")
    private static class Pojo {

        @SuppressWarnings("FieldCanBeLocal")
        private int field1;
        public String field2;

        public Pojo() {
        }

        public Pojo(int field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        public void setField1(int field1) {
            this.field1 = field1;
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
            return field1 == pojo.field1 &&
                    Objects.equals(field2, pojo.field2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2);
        }
    }
}
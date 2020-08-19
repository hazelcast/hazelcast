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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrimitiveUpsertTargetTest {

    @Test
    public void testInject() {
        UpsertTarget target = new PrimitiveUpsertTarget();
        UpsertInjector injector = target.createInjector(null);

        target.init();
        injector.set("value1");
        Object value1 = target.conclude();
        assertEquals("value1", value1);

        target.init();
        injector.set("value2");
        Object value2 = target.conclude();
        assertEquals("value2", value2);
    }
}
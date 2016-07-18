/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.io;

import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Category(QuickTest.class)
public class PredefinedTypeTest {

    private Object object;
    private PredefinedType predefinedType;

    public PredefinedTypeTest(Object object, PredefinedType type) {
        this.object = object;
        this.predefinedType = type;
    }

    @Parameterized.Parameters(name = "object={0}, type={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true, PredefinedType.BOOLEAN},
                {(byte) 0, PredefinedType.BYTE},
                {'c', PredefinedType.CHAR},
                {0.0d, PredefinedType.DOUBLE},
                {0.0f, PredefinedType.FLOAT},
                {0, PredefinedType.INT},
                {0L, PredefinedType.LONG},
                {(short) 0, PredefinedType.SHORT},
                {"string", PredefinedType.STRING},
                {null, PredefinedType.NULL},
                {new Tuple2<>(0, 0), PredefinedType.TUPLE2},
                {new Object(), PredefinedType.OBJECT}
        });
    }

    @Test
    public void testGetDataType() {
        assertEquals(predefinedType, PredefinedType.getDataType(object));
    }
}

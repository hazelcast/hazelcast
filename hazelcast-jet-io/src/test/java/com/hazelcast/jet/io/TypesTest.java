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
public class TypesTest {

    private Object object;
    private Types type;

    public TypesTest(Object object, Types type) {
        this.object = object;
        this.type = type;
    }

    @Parameterized.Parameters(name = "object={0}, type={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true, Types.BOOLEAN},
                {(byte) 0, Types.BYTE},
                {'c', Types.CHAR},
                {0.0d, Types.DOUBLE},
                {0.0f, Types.FLOAT},
                {0, Types.INT},
                {0L, Types.LONG},
                {(short) 0, Types.SHORT},
                {"string", Types.STRING},
                {null, Types.NULL},
                {new Tuple2<>(0, 0), Types.TUPLE},
                {new Object(), Types.OBJECT}
        });
    }

    @Test
    public void testGetDataType() {
        assertEquals(type, Types.getDataType(object));
    }
}

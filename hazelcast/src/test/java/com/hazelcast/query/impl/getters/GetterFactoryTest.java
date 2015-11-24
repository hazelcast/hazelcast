/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetterFactoryTest {

    private Field outerNameField;
    private Field innersField;
    private Field innerNameField;
    private Field innerAttributesField;

    @Before
    public void setUp() throws NoSuchFieldException {
        outerNameField = OuterObject.class.getDeclaredField("name");
        innersField = OuterObject.class.getDeclaredField("inners");
        innerNameField = InnerObject.class.getDeclaredField("name");
        innerAttributesField = InnerObject.class.getDeclaredField("attributes");
    }

    @Test
    public void newFieldGetter_whenExtractingFromSimpleField_thenInferTypeFromFieldType() throws Exception {
        OuterObject object = new OuterObject("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, outerNameField, null);

        Class returnType = getter.getReturnType();
        assertEquals(String.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromSimpleField_thenReturnFieldContentIsItIs() throws Exception {
        OuterObject object = new OuterObject("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, outerNameField, null);

        String result = (String) getter.getValue(object);
        assertEquals("name", result);
    }

    @Test
    public void newFieldGetter_whenExtractingFromEmptyCollectionAndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = new OuterObject("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, innersField, "[any]");

        assertSame(NullGetter.NULL_GETTER, getter);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmptyCollectionAndReducerSuffixInNotEmpty_thenInferTypeFromCollectionItem()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));
        Getter getter = GetterFactory.newFieldGetter(object, null, innersField, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromSimpleFieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerNameField, null);

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(String.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromEmptyCollectionFieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesField, "[any]");

        assertSame(NullGetter.NULL_GETTER, innerObjectNameGetter);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmptyCollectionFieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    public static class OuterObject {
        final String name;
        final Collection<InnerObject> inners;

        OuterObject(String name, InnerObject...inners) {
            this.name = name;
            this.inners = asList(inners);
        }
    }

    public static class InnerObject {
        final String name;
        final Collection<Integer> attributes;

        InnerObject(String name, Integer...attributes) {
            this.name = name;
            this.attributes = asList(attributes);
        }
    }
}

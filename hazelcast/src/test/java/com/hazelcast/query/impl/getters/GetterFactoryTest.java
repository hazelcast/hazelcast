/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GetterFactoryTest {

    private Field outerNameField;
    private Field innersCollectionField;
    private Field innersArrayField;
    private Field innerNameField;
    private Field innerAttributesCollectionField;
    private Field innerAttributesArrayField;

    private Method outerNameMethod;
    private Method innersCollectionMethod;
    private Method innersArrayMethod;
    private Method innerNameMethod;
    private Method innerAttributesCollectionMethod;
    private Method innerAttributesArrayMethod;

    @Before
    public void setUp() throws NoSuchFieldException, NoSuchMethodException {
        outerNameField = OuterObject.class.getDeclaredField("name");
        innersCollectionField = OuterObject.class.getDeclaredField("innersCollection");
        innersArrayField = OuterObject.class.getDeclaredField("innersArray");
        innerNameField = InnerObject.class.getDeclaredField("name");
        innerAttributesCollectionField = InnerObject.class.getDeclaredField("attributesCollection");
        innerAttributesArrayField = InnerObject.class.getDeclaredField("attributesArray");

        outerNameMethod = OuterObject.class.getDeclaredMethod("getName");
        innersCollectionMethod = OuterObject.class.getDeclaredMethod("getInnersCollection");
        innersArrayMethod = OuterObject.class.getDeclaredMethod("getInnersArray");
        innerNameMethod = InnerObject.class.getDeclaredMethod("getName");
        innerAttributesCollectionMethod = InnerObject.class.getDeclaredMethod("getAttributesCollection");
        innerAttributesArrayMethod = InnerObject.class.getDeclaredMethod("getAttributesArray");
    }

    @Test
    public void newFieldGetter_whenExtractingFromSimpleField_thenInferTypeFromFieldType() throws Exception {
        OuterObject object = new OuterObject("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, outerNameField, null);

        Class returnType = getter.getReturnType();
        assertEquals(String.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromSimpleField_thenInferTypeFromFieldType() throws Exception {
        OuterObject object = new OuterObject("name");
        Getter getter = GetterFactory.newMethodGetter(object, null, outerNameMethod, null);

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
    public void newMethodGetter_whenExtractingFromSimpleField_thenReturnFieldContentIsItIs() throws Exception {
        OuterObject object = new OuterObject("name");
        Getter getter = GetterFactory.newMethodGetter(object, null, outerNameMethod, null);

        String result = (String) getter.getValue(object);
        assertEquals("name", result);
    }

    @Test
    public void newFieldGetter_whenExtractingFromEmpty_Collection_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.emptyInner("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, getter);
    }

    @Test
    public void newMethodGetter_whenExtractingFromEmpty_Collection_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.emptyInner("name");
        Getter getter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, getter);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNull_Collection_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.nullInner("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, getter);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNull_Collection_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.nullInner("name");
        Getter getter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, getter);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Collection_AndReducerSuffixInNotEmpty_thenInferTypeFromCollectionItem()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));
        Getter getter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Collection_AndReducerSuffixInNotEmpty_thenInferTypeFromCollectionItem()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));
        Getter getter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromEmpty_Array_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.emptyInner("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromEmpty_Array_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.emptyInner("name");
        Getter getter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNull_Array_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.nullInner("name");
        Getter getter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNull_Array_AndReducerSuffixInNotEmpty_thenReturnNullGetter()
            throws Exception {
        OuterObject object = OuterObject.nullInner("name");
        Getter getter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Array_AndReducerSuffixInNotEmpty_thenInferTypeFromCollectionItem()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));
        Getter getter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Array_AndReducerSuffixInNotEmpty_thenInferTypeFromCollectionItem()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));
        Getter getter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");

        Class returnType = getter.getReturnType();
        assertEquals(InnerObject.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromSimpleFieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerNameField, null);

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(String.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromSimpleFieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner"));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newMethodGetter(object, parentGetter, innerNameMethod, null);

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(String.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromEmpty_Collection_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.emptyInner("inner"));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesCollectionField, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, innerObjectNameGetter);
    }

    @Test
    public void newMethodGetter_whenExtractingFromEmpty_Collection_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.emptyInner("inner"));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesCollectionMethod, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, innerObjectNameGetter);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNull_Collection_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.nullInner("inner"));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesCollectionField, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, innerObjectNameGetter);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNull_Collection_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.nullInner("inner"));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesCollectionMethod, "[any]");

        assertSame(NullMultiValueGetter.NULL_MULTIVALUE_GETTER, innerObjectNameGetter);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Collection_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesCollectionField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Collection_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesCollectionMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Collection_nullFirst_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesCollectionField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Collection_nullFirst_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesCollectionMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Collection_FieldAndParentIsNonEmptyMultiResult_nullValueFirst_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesCollectionField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Collection_FieldAndParentIsNonEmptyMultiResult_nullValueFirst_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesCollectionMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Collection_bothNullFirst_FieldAndParentIsNonEmptyMultiResult_nullValueFirst_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersCollectionField, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesCollectionField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Collection_bothNullFirst_FieldAndParentIsNonEmptyMultiResult_nullValueFirst_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersCollectionMethod, "[any]");
        Getter innerObjectNameGetter
                = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesCollectionMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromEmpty_Array_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.emptyInner("inner"));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesArrayField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromEmpty_Array_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.emptyInner("inner"));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesArrayMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNull_Array_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.nullInner("inner"));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesArrayField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNull_Array_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", InnerObject.nullInner("inner"));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesArrayMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Array_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesArrayField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Array_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesArrayMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Array_nullFirst_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesArrayField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Array_nullFirst_FieldAndParentIsNonEmptyMultiResult_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesArrayMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Array_FieldAndParentIsNonEmptyMultiResult_nullFirstValue_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesArrayField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Array_FieldAndParentIsNonEmptyMultiResult_nullFirstValue_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesArrayMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newFieldGetter_whenExtractingFromNonEmpty_Array_bothNullFirst_FieldAndParentIsNonEmptyMultiResult_nullFirstValue_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newFieldGetter(object, null, innersArrayField, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newFieldGetter(object, parentGetter, innerAttributesArrayField, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newMethodGetter_whenExtractingFromNonEmpty_Array_bothNullFirst_FieldAndParentIsNonEmptyMultiResult_nullFirstValue_thenInferReturnType()
            throws Exception {
        OuterObject object = new OuterObject("name", null, new InnerObject("inner", null, 0, 1, 2, 3));

        Getter parentGetter = GetterFactory.newMethodGetter(object, null, innersArrayMethod, "[any]");
        Getter innerObjectNameGetter = GetterFactory.newMethodGetter(object, parentGetter, innerAttributesArrayMethod, "[any]");

        Class returnType = innerObjectNameGetter.getReturnType();
        assertEquals(Integer.class, returnType);
    }

    @Test
    public void newThisGetter() {
        OuterObject object = new OuterObject("name", new InnerObject("inner", 0, 1, 2, 3));
        Getter innerObjectThisGetter = GetterFactory.newThisGetter(null, object);

        Class returnType = innerObjectThisGetter.getReturnType();
        assertEquals(OuterObject.class, returnType);
    }

    public static class OuterObject {

        final String name;
        final Collection<InnerObject> innersCollection;
        final InnerObject[] innersArray;

        OuterObject(String name, InnerObject... innersCollection) {
            this.name = name;
            this.innersCollection = asList(innersCollection);
            this.innersArray = innersCollection;
        }

        OuterObject(String name, InnerObject[] attributesArray, Collection<InnerObject> attributesCollection) {
            this.name = name;
            this.innersCollection = attributesCollection;
            this.innersArray = attributesArray;
        }

        public static OuterObject nullInner(String name) {
            return new OuterObject(name, null, null);
        }

        public static OuterObject emptyInner(String name) {
            return new OuterObject(name, new InnerObject[0], new ArrayList<InnerObject>());
        }

        public String getName() {
            return name;
        }

        public Collection<InnerObject> getInnersCollection() {
            return innersCollection;
        }

        public InnerObject[] getInnersArray() {
            return innersArray;
        }
    }

    public static class InnerObject {

        final String name;
        final Collection<Integer> attributesCollection;
        final Integer[] attributesArray;

        InnerObject(String name, Integer[] attributesArray, Collection<Integer> attributesCollection) {
            this.name = name;
            this.attributesCollection = attributesCollection;
            this.attributesArray = attributesArray;
        }

        InnerObject(String name, Integer... attributesCollection) {
            this.name = name;
            this.attributesCollection = asList(attributesCollection);
            this.attributesArray = attributesCollection;
        }

        public static InnerObject nullInner(String name) {
            return new InnerObject(name, null, null);
        }

        public static InnerObject emptyInner(String name) {
            return new InnerObject(name, new Integer[0], new ArrayList<Integer>());
        }

        public String getName() {
            return name;
        }

        public Collection<Integer> getAttributesCollection() {
            return attributesCollection;
        }

        public Integer[] getAttributesArray() {
            return attributesArray;
        }
    }
}

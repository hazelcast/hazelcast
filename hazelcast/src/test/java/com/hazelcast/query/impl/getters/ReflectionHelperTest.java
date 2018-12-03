/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.QueryException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.query.impl.getters.ReflectionHelper.resolveActualTypeArguments;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReflectionHelperTest {

    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_thenThrowIllegalArgumentException() throws
            Exception {
        OuterObject object = new OuterObject();
        try {
            ReflectionHelper.extractValue(object, "emptyInterface.doesNotExist");
            fail("Non-existing field has been ignored");
        } catch (QueryException e) {
            // createGetter() method is catching everything throwable and wraps it in QueryException
            // I don't think it's the right thing to do, but I don't want to change this behaviour.
            // Hence I have to use try/catch in this test instead of just declaring
            // IllegalArgumentException as expected exception.
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        }
    }

    private static class OuterObject {
        @SuppressWarnings("unused")
        private EmptyInterface emptyInterface;
    }

    private interface EmptyInterface {
    }

    @Test
    public void testActualTypeArgumentsResolution_onClasses() {
        Class[] resolved;

        resolved = resolveActualTypeArguments(HashMap.class, Collection.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(HashMap.class, Iterable.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(HashMap.class, ArrayList.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(Collection.class, ArrayList.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(ArrayList.class, Collection.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(HashMap.class, Map.class);
        assertArrayEquals(new Class[]{null, null}, resolved);

        resolved = resolveActualTypeArguments(IntegerArrayList.class, IntegerArrayList.class);
        assertArrayEquals(new Class[]{}, resolved);

        resolved = resolveActualTypeArguments(IntegerArrayList.class, Collection.class);
        assertArrayEquals(new Class[]{Integer.class}, resolved);

        resolved = resolveActualTypeArguments(IntegerToLongHashMap.class, Map.class);
        assertArrayEquals(new Class[]{Integer.class, Long.class}, resolved);

        resolved = resolveActualTypeArguments(IntegerToAnyHashMap.class, Map.class);
        assertArrayEquals(new Class[]{Integer.class, null}, resolved);

        resolved = resolveActualTypeArguments(AnyToStringHashMap.class, AnyToStringHashMap.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(AnyToStringHashMap.class, Map.class);
        assertArrayEquals(new Class[]{null, String.class}, resolved);

        resolved = resolveActualTypeArguments(IterableHashMap.class, Iterable.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(IntegerToStringIterableHashMap.class, Iterable.class);
        assertArrayEquals(new Class[]{String.class}, resolved);

        resolved = resolveActualTypeArguments(IntegerToStringIterableHashMap.class, Map.class);
        assertArrayEquals(new Class[]{Integer.class, String.class}, resolved);

        resolved = resolveActualTypeArguments(Container.class, Container.class);
        assertArrayEquals(new Class[]{null, null}, resolved);

        resolved = resolveActualTypeArguments(Container.InnerHashMap.class, Map.class);
        assertArrayEquals(new Class[]{null, null}, resolved);

        resolved = resolveActualTypeArguments(Container.InnerIntegerValuedHashMap.class, Map.class);
        assertArrayEquals(new Class[]{null, Integer.class}, resolved);

        resolved = resolveActualTypeArguments(StringToBooleanContainer.class, Container.class);
        assertArrayEquals(new Class[]{String.class, Boolean.class}, resolved);

        resolved = resolveActualTypeArguments(StringToBooleanContainer.InnerHashMap.class, Map.class);
        // InnerHashMap is not referring a specialized type here
        assertArrayEquals(new Class[]{null, null}, resolved);

        resolved = resolveActualTypeArguments(StringToBooleanContainer.InnerIntegerValuedHashMap.class, Map.class);
        // InnerIntegerValuedHashMap is not referring a specialized type here
        assertArrayEquals(new Class[]{null, Integer.class}, resolved);

        resolved = resolveActualTypeArguments(StringToBooleanContainer.InnerLongValuedHashMap.class, Map.class);
        assertArrayEquals(new Class[]{String.class, Long.class}, resolved);
    }

    private static class IntegerArrayList extends ArrayList<Integer> {
    }

    private static class IntegerToLongHashMap extends HashMap<Integer, Long> {
    }

    private static class IntegerToAnyHashMap<Value> extends HashMap<Integer, Value> {
    }

    private static class AnyToStringHashMap<Key> extends HashMap<Key, String> {
    }

    private static class IterableHashMap<K, V> extends HashMap<K, V> implements Iterable<V> {
        @SuppressWarnings("NullableProblems")
        @Override
        public Iterator<V> iterator() {
            return values().iterator();
        }
    }

    private static class IntegerToStringIterableHashMap extends IterableHashMap<Integer, String> {
    }

    @SuppressWarnings("unused")
    private static class Container<K, V> {
        @SuppressWarnings("TypeParameterHidesVisibleType")
        public class InnerHashMap<V> extends HashMap<K, V> {
        }

        public class InnerIntegerValuedHashMap extends InnerHashMap<Integer> {
        }
    }

    private static class StringToBooleanContainer extends Container<String, Boolean> {
        public class InnerLongValuedHashMap extends InnerHashMap<Long> {
        }
    }

    @Test
    public void testActualTypeArgumentsResolution_onInterfaces() {
        Class[] resolved;

        resolved = resolveActualTypeArguments(Map.class, Collection.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(Map.class, Iterable.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(Map.class, HashMap.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(HashMap.Entry.class, Map.Entry.class);
        assertArrayEquals(new Class[]{null, null}, resolved);

        resolved = resolveActualTypeArguments(List.class, Collection.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(ConcurrentMap.class, Map.class);
        assertArrayEquals(new Class[]{null, null}, resolved);

        resolved = resolveActualTypeArguments(IntegerList.class, IntegerList.class);
        assertArrayEquals(new Class[]{}, resolved);

        resolved = resolveActualTypeArguments(IntegerList.class, Collection.class);
        assertArrayEquals(new Class[]{Integer.class}, resolved);

        resolved = resolveActualTypeArguments(IntegerToLongMap.class, Map.class);
        assertArrayEquals(new Class[]{Integer.class, Long.class}, resolved);

        resolved = resolveActualTypeArguments(IntegerToAnyMap.class, Map.class);
        assertArrayEquals(new Class[]{Integer.class, null}, resolved);

        resolved = resolveActualTypeArguments(AnyToStringMap.class, Map.class);
        assertArrayEquals(new Class[]{null, String.class}, resolved);

        resolved = resolveActualTypeArguments(IterableMap.class, Iterable.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(IntegerToStringIterableMap.class, Iterable.class);
        assertArrayEquals(new Class[]{String.class}, resolved);

        resolved = resolveActualTypeArguments(IntegerToStringIterableMap.class, Map.class);
        assertArrayEquals(new Class[]{Integer.class, String.class}, resolved);

        resolved = resolveActualTypeArguments(IntegerToStringIterableMap.class, Map.class);
        assertArrayEquals(new Class[]{Integer.class, String.class}, resolved);

        resolved = resolveActualTypeArguments(Outer.Inner.class, Outer.Inner.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(Outer.IntegerInner.class, Outer.Inner.class);
        assertArrayEquals(new Class[]{Integer.class}, resolved);

        resolved = resolveActualTypeArguments(Outer.IntegerInner.class, Outer.class);
        assertArrayEquals(null, resolved);

        resolved = resolveActualTypeArguments(LongOuterIntegerInner.class, Outer.class);
        assertArrayEquals(new Class[]{Long.class}, resolved);

        resolved = resolveActualTypeArguments(LongOuterIntegerInner.class, Outer.Inner.class);
        assertArrayEquals(new Class[]{Integer.class}, resolved);
    }

    private interface IntegerList extends List<Integer> {
    }

    private interface IntegerToLongMap extends Map<Integer, Long> {
    }

    private interface IntegerToAnyMap<Value> extends Map<Integer, Value> {
    }

    private interface AnyToStringMap<Key> extends Map<Key, String> {
    }

    private interface IterableMap<K, V> extends Map<K, V>, Iterable<V> {
    }

    private interface IntegerToStringIterableMap extends IterableMap<Integer, String> {
    }

    @SuppressWarnings("unused")
    private interface Outer<V> {
        interface Inner<V> {
        }

        interface IntegerInner extends Inner<Integer> {
        }
    }

    interface LongOuterIntegerInner extends Outer<Long>, Outer.Inner<Integer> {
    }

    @Test
    public void testActualTypeArgumentsResolution_withComplexTypeBounds() throws NoSuchFieldException, NoSuchMethodException {
        Class[] resolved;

        resolved = resolveActualTypeArguments(ComplexBounds.class, Comparable.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(ComplexBounds.class, Iterable.class);
        assertArrayEquals(new Class[]{null}, resolved);

        resolved = resolveActualTypeArguments(ComplexBounds.class, Map.class);
        assertArrayEquals(new Class[]{null, null}, resolved);

        resolved = resolveActualTypeArguments(ComplexBoundsSpecialization.class, ComplexBounds.class);
        assertArrayEquals(new Class[]{Number.class, Long.class, Double.class}, resolved);

        resolved = resolveActualTypeArguments(ComplexBoundsSpecialization.class, Comparable.class);
        assertArrayEquals(new Class[]{Number.class}, resolved);

        resolved = resolveActualTypeArguments(ComplexBoundsSpecialization.class, Iterable.class);
        assertArrayEquals(new Class[]{Long.class}, resolved);

        resolved = resolveActualTypeArguments(ComplexBoundsSpecialization.class, Map.class);
        assertArrayEquals(new Class[]{Long.class, Double.class}, resolved);

        Type complexBoundsField = ComplexBoundsMembers.class.getField("complexBoundsField").getGenericType();
        resolved = resolveActualTypeArguments(complexBoundsField, ComplexBounds.class);
        assertArrayEquals(new Class[]{Number.class, Integer.class, BigDecimal.class}, resolved);

        resolved = resolveActualTypeArguments(complexBoundsField, Comparable.class);
        assertArrayEquals(new Class[]{Number.class}, resolved);

        resolved = resolveActualTypeArguments(complexBoundsField, Iterable.class);
        assertArrayEquals(new Class[]{Integer.class}, resolved);

        resolved = resolveActualTypeArguments(complexBoundsField, Map.class);
        assertArrayEquals(new Class[]{Integer.class, BigDecimal.class}, resolved);

        Type complexBoundsMethod = ComplexBoundsMembers.class.getMethod("complexBoundsMethod").getGenericReturnType();
        resolved = resolveActualTypeArguments(complexBoundsMethod, ComplexBounds.class);
        assertArrayEquals(new Class[]{Number.class, Integer.class, BigDecimal.class}, resolved);

        resolved = resolveActualTypeArguments(complexBoundsMethod, Comparable.class);
        assertArrayEquals(new Class[]{Number.class}, resolved);

        resolved = resolveActualTypeArguments(complexBoundsMethod, Iterable.class);
        assertArrayEquals(new Class[]{Integer.class}, resolved);

        resolved = resolveActualTypeArguments(complexBoundsMethod, Map.class);
        assertArrayEquals(new Class[]{Integer.class, BigDecimal.class}, resolved);
    }

    private interface ComplexBounds<B1 extends Serializable, B2 extends B1, B3 extends Number & Comparable<B3>>
            extends Comparable<B1>, Iterable<B2>, Map<B2, B3> {
    }

    private static class ComplexBoundsSpecialization extends HashMap<Long, Double>
            implements ComplexBounds<Number, Long, Double> {
        @SuppressWarnings("NullableProblems")
        @Override
        public int compareTo(Number o) {
            return 0;
        }

        @SuppressWarnings({"NullableProblems", "ConstantConditions"})
        @Override
        public Iterator<Long> iterator() {
            return null;
        }
    }

    private static class ComplexBoundsMembers {

        public final ComplexBounds<Number, Integer, BigDecimal> complexBoundsField = null;

        public ComplexBounds<Number, Integer, BigDecimal> complexBoundsMethod() {
            return null;
        }

    }

}

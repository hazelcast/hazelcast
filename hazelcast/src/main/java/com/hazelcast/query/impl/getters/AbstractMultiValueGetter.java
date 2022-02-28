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

import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.collection.ArrayUtils;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.util.Collection;

import static com.hazelcast.query.impl.predicates.PredicateUtils.unwrapIfOptional;

public abstract class AbstractMultiValueGetter extends Getter {

    public static final String REDUCER_ANY_TOKEN = "any";

    public static final int DO_NOT_REDUCE = -1;
    public static final int REDUCE_EVERYTHING = -2;

    private final int modifier;
    private final Class resultType;

    public AbstractMultiValueGetter(Getter parent, String modifierSuffix, Class<?> inputType, Class resultType) {
        super(parent);
        boolean isArray = inputType.isArray();
        boolean isCollection = Collection.class.isAssignableFrom(inputType);
        if (modifierSuffix == null) {
            modifier = DO_NOT_REDUCE;
        } else {
            modifier = parseModifier(modifierSuffix, isArray, isCollection);
        }
        this.resultType = getResultType(inputType, resultType);
    }

    protected abstract Object extractFrom(Object parentObject) throws IllegalAccessException, InvocationTargetException;

    @Override
    Class getReturnType() {
        return resultType;
    }

    @Override
    Object getValue(Object obj) throws Exception {
        Object parentObject = getParentObject(obj);
        if (parentObject == null) {
            return null;
        }
        if (parentObject instanceof MultiResult) {
            return extractFromMultiResult((MultiResult) parentObject);
        }

        Object o = unwrapIfOptional(extractFrom(parentObject));
        if (modifier == DO_NOT_REDUCE) {
            return o;
        }
        if (modifier == REDUCE_EVERYTHING) {
            MultiResult<Object> collector = new MultiResult<>();
            reduceInto(collector, o);
            return collector;
        }
        return getItemAtPositionOrNull(o, modifier);
    }

    protected int getModifier() {
        return modifier;
    }

    private Class getResultType(Class inputType, Class resultType) {
        if (resultType != null) {
            //result type as been set explicitly via Constructor.
            //This is needed for extraction Collection where type cannot be
            //inferred due type erasure
            return resultType;
        }

        if (modifier == DO_NOT_REDUCE) {
            //We are returning the object as it is.
            //No modifier suffix was defined
            return inputType;
        }

        if (!inputType.isArray()) {
            throw new IllegalArgumentException(
                    "Cannot infer a return type with modifier " + modifier + " on type " + inputType.getName());
        }

        //ok, it must be an array. let's return array type
        return inputType.getComponentType();
    }

    private void collectResult(MultiResult<Object> collector, Object parentObject) throws IllegalAccessException,
            InvocationTargetException {
        // re-add nulls from parent extraction without extracting further down the path
        if (parentObject == null) {
            collector.add(null);
        } else {
            Object currentObject = unwrapIfOptional(extractFrom(parentObject));
            if (shouldReduce()) {
                reduceInto(collector, currentObject);
            } else {
                collector.add(currentObject);
            }
        }
    }

    private Object extractFromMultiResult(MultiResult parentMultiResult) throws IllegalAccessException,
            InvocationTargetException {
        MultiResult<Object> collector = new MultiResult<>();
        collector.setNullOrEmptyTarget(parentMultiResult.isNullEmptyTarget());
        for (Object result : parentMultiResult.getResults()) {
            collectResult(collector, result);
        }

        return collector;
    }

    private boolean shouldReduce() {
        return modifier != DO_NOT_REDUCE;
    }

    private int parseModifier(String modifierSuffix, boolean isArray, boolean isCollection) {
        if (!isArray && !isCollection) {
            throw new IllegalArgumentException("Reducer is allowed only when extracting from arrays or collections");
        }
        return parseModifier(modifierSuffix);
    }

    private Object getItemAtPositionOrNull(Object object, int position) {
        if (object == null) {
            return null;
        } else if (object instanceof Collection) {
            return CollectionUtil.getItemAtPositionOrNull((Collection<?>) object, position);
        } else if (object instanceof Object[]) {
            return ArrayUtils.getItemAtPositionOrNull((Object[]) object, position);
        } else if (object.getClass().isArray()) {
            return Array.get(object, position);
        }
        throw new IllegalArgumentException("Cannot extract an element from class of type" + object.getClass()
                + " Collections and Arrays are supported only");
    }

    private Object getParentObject(Object obj) throws Exception {
        return parent != null ? parent.getValue(obj) : obj;
    }

    private void reduceArrayInto(MultiResult<Object> collector, Object[] array) {
        if (array.length == 0) {
            collector.addNullOrEmptyTarget();
        } else {
            for (Object o : array) {
                collector.add(o);
            }
        }
    }

    protected void reduceCollectionInto(MultiResult<Object> collector, Collection collection) {
        if (collection.isEmpty()) {
            collector.addNullOrEmptyTarget();
        } else {
            for (Object o : collection) {
                collector.add(o);
            }
        }
    }

    protected void reduceInto(MultiResult<Object> collector, Object currentObject) {
        if (modifier != REDUCE_EVERYTHING) {
            Object item = getItemAtPositionOrNull(currentObject, modifier);
            collector.add(item);
            return;
        }

        if (currentObject == null) {
            collector.addNullOrEmptyTarget();
        } else if (currentObject instanceof Collection) {
            reduceCollectionInto(collector, (Collection) currentObject);
        } else if (currentObject instanceof Object[]) {
            reduceArrayInto(collector, (Object[]) currentObject);
        } else if (currentObject.getClass().isArray()) {
            if (!ExtractorHelper.reducePrimitiveArrayInto(collector::add, currentObject)) {
                collector.addNullOrEmptyTarget();
            }
        } else {
            throw new IllegalArgumentException("Can't reduce result from a type " + currentObject.getClass()
                    + " Only Collections and Arrays are supported.");
        }
    }

    private static int parseModifier(String modifier) {
        String stringValue = modifier.substring(1, modifier.length() - 1);
        if (REDUCER_ANY_TOKEN.equals(stringValue)) {
            return REDUCE_EVERYTHING;
        }

        int pos = Integer.parseInt(stringValue);
        if (pos < 0) {
            throw new IllegalArgumentException("Position argument cannot be negative. Passed argument: " + modifier);
        }
        return pos;
    }

    static void validateModifier(String modifier) {
        parseModifier(modifier);
    }

    protected static String composeAttributeValueExtractionFailedMessage(Member member) {
        return "Attribute value extraction failed for: " + member + ". Make "
                + "sure attribute values or collection/array attribute value "
                + "elements are all of the same concrete type. Consider custom "
                + "attribute extractors if it's impossible or undesirable to "
                + "reduce the variety of types to a single type, see Custom "
                + "Attributes section in the reference manual for more details.";
    }

}

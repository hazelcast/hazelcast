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


import com.hazelcast.query.extractor.MultiResult;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.collection.ArrayUtils;

import java.lang.reflect.Field;
import java.util.Collection;

public class FieldGetter extends Getter {
    private static final int DO_NOT_REDUCE = -1;
    private static final int REDUCE_EVERYTHING = -2;

    private final Field field;
    private final int modifier;
    private final Class resultType;

    public FieldGetter(Getter parent, Field field, String modifierSuffix, Class resultType) {
        super(parent);
        this.field = field;
        boolean isArray = field.getType().isArray();
        boolean isCollection = Collection.class.isAssignableFrom(field.getType());

        if (modifierSuffix == null) {
            modifier = DO_NOT_REDUCE;
        } else {
            modifier = parseModifier(modifierSuffix, isArray, isCollection);
        }

        this.resultType = getResultType(field, resultType);
    }

    private int parseModifier(String modifierSuffix, boolean isArray, boolean isCollection) {
        if (!isArray && !isCollection) {
            throw new IllegalStateException("Reducer is allowed only when extracting from arrays or collections");
        }
        String stringValue = modifierSuffix.substring(1, modifierSuffix.length() - 1);
        if ("*".equals(stringValue)) {
            return REDUCE_EVERYTHING;
        } else {
            return Integer.parseInt(stringValue);
        }
    }

    public FieldGetter(Getter parent, Field field, String reducer) {
        this(parent, field, reducer, null);
    }

    private Class getResultType(Field field, Class resultType) {
        if (resultType != null) {
            //result type as been set explicitly via Constructor.
            //This is needed for extraction Collection where type cannot be
            //inferred due type erasure
            return resultType;
        }
        if (modifier == DO_NOT_REDUCE) {
            //We are returning the object as it is.
            //No modifier suffix was defined
            return field.getType();
        }

        //ok, it must be an array. let's return array type
        return field.getType().getComponentType();
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

        Object o = field.get(parentObject);
        if (modifier == DO_NOT_REDUCE) {
            return o;
        }
        if (modifier == REDUCE_EVERYTHING) {
            MultiResult collector = new MultiResult();
            reduceInto(collector, o);
            return collector;
        }
        return getItemAtPositionOrNull(o, modifier);
    }

    private Object extractFromMultiResult(MultiResult parentMultiResult) throws IllegalAccessException {
        MultiResult collector = new MultiResult();
        for (Object parentResult : parentMultiResult.getResults()) {
            collectResult(collector, parentResult);
        }

        return collector;
    }

    private void collectResult(MultiResult collector, Object parentObject) throws IllegalAccessException {
        Object currentObject = field.get(parentObject);
        if (currentObject == null) {
            return;
        }
        if (shouldReduce()) {
            reduceInto(collector, currentObject);
        } else {
            collector.add(currentObject);
        }
    }

    private void reduceInto(MultiResult collector, Object currentObject) {
        if (modifier != REDUCE_EVERYTHING) {
            Object item = getItemAtPositionOrNull(currentObject, modifier);
            collector.add(item);
            return;
        }

        if (currentObject instanceof Collection) {
            reduceCollectionInto(collector, (Collection) currentObject);
        } else if (currentObject instanceof Object[]) {
            reduceArrayInto(collector, (Object[]) currentObject);
        } else {
            throw new IllegalArgumentException("Can't reduce result from a type " + currentObject.getClass()
                    + " Only Collections and Arrays are supported.");
        }
    }

    private void reduceArrayInto(MultiResult collector, Object[] currentObject) {
        Object[] array = currentObject;
        for (int i = 0; i < array.length; i++) {
            collector.add(array[i]);
        }
    }

    private void reduceCollectionInto(MultiResult collector, Collection currentObject) {
        Collection collection = currentObject;
        for (Object o : collection) {
            collector.add(o);
        }
    }

    private Object getItemAtPositionOrNull(Object object, int position) {
        if (object instanceof Collection) {
            return CollectionUtil.getItemAtPositionOrNull((Collection) object, position);
        } else if (object instanceof Object[]) {
            return ArrayUtils.getItemAtPositionOrNull((Object[]) object, position);
        }
        throw new IllegalArgumentException("Cannot extract an element from class of type" + object.getClass()
                + " Collections and Arrays are supported only");
    }

    private boolean shouldReduce() {
        return modifier != DO_NOT_REDUCE;
    }


    private Object getParentObject(Object obj) throws Exception {
        return parent != null ? parent.getValue(obj) : obj;
    }

    @Override
    Class getReturnType() {
        return resultType;
    }

    @Override
    boolean isCacheable() {
        return ReflectionHelper.THIS_CL.equals(field.getDeclaringClass().getClassLoader());
    }

    @Override
    public String toString() {
        return "FieldGetter [parent=" + parent + ", field=" + field + "]";
    }
}

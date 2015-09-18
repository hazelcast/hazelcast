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


import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FieldGetter extends Getter {
    private static final int DO_NOT_REDUCE = -1;
    private static final int REDUCE_EVERYTHING = -2;

    private final Field field;
    private final int position;
    private final Class resultType;

    public FieldGetter(Getter parent, Field field, String reducer, Class resultType) {
        super(parent);
        this.field = field;
        boolean isArray = field.getType().isArray();
        boolean isCollection = Collection.class.isAssignableFrom(field.getType());

        if (reducer != null) {
            if (!isArray && !isCollection) {
                throw new IllegalStateException("Reducer is allowed only when extracting from arrays or collections");
            }
            String stringValue = reducer.substring(1, reducer.length() - 1);
            if ("*".equals(stringValue)) {
                position = REDUCE_EVERYTHING;
            } else {
                position = Integer.parseInt(stringValue);
            }
        } else {
            position = DO_NOT_REDUCE;
        }

        this.resultType = createResultType(field, resultType);
    }

    private Class createResultType(Field field, Class resultType) {
        if (resultType != null) {
            //result type as been set explicitly
            return resultType;
        }
        if (position == DO_NOT_REDUCE) {
            return field.getType();
        }
        return field.getType().getComponentType();
    }


    public FieldGetter(Getter parent, Field field, String reducer) {
        this(parent, field, reducer, null);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        Object parentObject = getParentObject(obj);
        if (parentObject == null) {
            return null;
        }
        if (parentObject instanceof MultiResultCollector) {
            return extractFromMultiResult((MultiResultCollector) parentObject);
        }

        Object o = field.get(parentObject);
        if (position == DO_NOT_REDUCE) {
            return o;
        }
        if (position == REDUCE_EVERYTHING) {
            MultiResultCollector collector = new MultiResultCollector();
            reduceInto(collector, o);
            return collector;
        }
        return getItemAtPosition(o, position);
    }

    private Object extractFromMultiResult(MultiResultCollector parentMultiResult) throws IllegalAccessException {
        MultiResultCollector collector = new MultiResultCollector();
        for (Object parentResult : parentMultiResult.getResults()) {
            collectResult(collector, parentResult);
        }

        return collector;
    }

    private void collectResult(MultiResultCollector collector, Object parentResult) throws IllegalAccessException {
        Object currentObject = field.get(parentResult);
        if (currentObject == null) {
            return;
        }
        if (shouldReduce()) {
            reduceInto(collector, currentObject);
        } else {
            collector.collect(currentObject);
        }
    }

    private void reduceInto(MultiResultCollector collector, Object currentObject) {
        if (position != REDUCE_EVERYTHING) {
            Object item = getItemAtPosition(currentObject, position);
            collector.collect(item);
            return;
        }

        if (currentObject instanceof Collection) {
            Collection collection = (Collection) currentObject;
            for (Object o : collection) {
                collector.collect(o);
            }
        } else if (currentObject instanceof Object[]) {
            Object[] array = (Object[]) currentObject;
            for (int i = 0; i < array.length; i++) {
                collector.collect(array[i]);
            }
        } else {
            throw new IllegalArgumentException("Can't reduce result from a type " + currentObject.getClass()
                    + " Collections and Arrays are supported only");
        }
    }

    private Object getItemAtPosition(Object object, int position) {
        if (object instanceof Collection) {
            Collection collection = (Collection) object;
            if (collection.size() > position) {
                if (collection instanceof List) {
                    return ((List) collection).get(position);
                } else {
                    Iterator iterator = collection.iterator();
                    Object item = null;
                    for (int i = 0; i < position + 1; i++) {
                        item = iterator.next();
                    }
                    return item;
                }
            }
            return null;
        } else if (object instanceof Object[]) {
            Object[] array = (Object[]) object;
            if (array.length > position) {
                return array[position];
            }
            return null;
        }
        throw new IllegalArgumentException("Cannot extract an element from class of type" + object.getClass()
                + " Collections and Arrays are supported only");
    }

    private boolean shouldReduce() {
        return position != DO_NOT_REDUCE;
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

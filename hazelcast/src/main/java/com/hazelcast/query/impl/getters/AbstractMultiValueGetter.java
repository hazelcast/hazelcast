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

import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.collection.ArrayUtils;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;


public abstract class AbstractMultiValueGetter extends Getter {

    public static final String REDUCER_ANY_TOKEN = "any";
    public static final String REDUCER_ANY_TOKEN_EMBRACED = "[any]";

    public static final int DO_NOT_REDUCE = -1;
    public static final int REDUCE_EVERYTHING = -2;
    public static final int MODIFIER_NOT_USED = -3;

    static final String WRONG_MODIFIER_SUFFIX_ERROR = "Only non-empty reducers allowed for maps. ['value'] or"
            + " [any]. Not: %s";

    private static final String UNSET_VALUE = "";

    private static final int STRING_OFFSET = 2;
    private static final int BRACKETS_LENGTH = 4;

    private final int modifier;
    private final String mapKey;
    private final ReduceType reduceType;
    private final Class resultType;

    public AbstractMultiValueGetter(Getter parent, String modifierSuffix, Class<?> inputType, Class resultType) {
        super(parent);

        reduceType = determineReduceType(inputType, modifierSuffix);

        switch (reduceType) {
            case REDUCE_BY_INDEX:
                modifier = parseModifier(modifierSuffix);
                mapKey = UNSET_VALUE;
                break;
            case REDUCE_BY_MAP_KEY:
                modifier = MODIFIER_NOT_USED;
                mapKey = parseMapKey(modifierSuffix);
                break;
            case DO_NOT_REDUCE:
                modifier = DO_NOT_REDUCE;
                mapKey = UNSET_VALUE;
                break;
            case REDUCE_EVERYTHING:
                modifier = REDUCE_EVERYTHING;
                mapKey = UNSET_VALUE;
                break;
            default:
                throw new IllegalStateException("Getter incorrectly initialized `reduceType` to " + reduceType);
        }

        this.resultType = getResultType(inputType, resultType);
    }

    private ReduceType determineReduceType(Class<?> inputType, String modifierSuffix) {
        if (modifierSuffix == null) {
            return ReduceType.DO_NOT_REDUCE;
        }
        boolean isArray = inputType.isArray();
        boolean isCollection = Collection.class.isAssignableFrom(inputType);
        boolean isMap = Map.class.isAssignableFrom(inputType);
        if (isMap || isArray || isCollection) {
            String stringValue = removeBrackets(modifierSuffix);
            if (REDUCER_ANY_TOKEN.equals(stringValue)) {
                return ReduceType.REDUCE_EVERYTHING;
            }
            if (isMap) {
                return ReduceType.REDUCE_BY_MAP_KEY;
            } else {
                return ReduceType.REDUCE_BY_INDEX;
            }
        }

        throw new IllegalArgumentException("Reducer is allowed only when extracting from arrays, collections or maps");
    }

    private static String removeBrackets(String modifierSuffix) {
        return modifierSuffix.substring(1, modifierSuffix.length() - 1);
    }

    private String parseMapKey(String modifierSuffix) {
        validateMapKeyIsString(modifierSuffix);
        return modifierSuffix.substring(STRING_OFFSET, modifierSuffix.length() - STRING_OFFSET);
    }

    public static void validateMapKey(String modifierSuffix) {
        if (REDUCER_ANY_TOKEN_EMBRACED.equals(modifierSuffix)) {
            return;
        }

        validateMapKeyIsString(modifierSuffix);
    }

    private static void validateMapKeyIsString(String modifierSuffix) {
        if (!modifierSuffix.startsWith("['") || !modifierSuffix.endsWith("']")
                || modifierSuffix.length() <= BRACKETS_LENGTH) {
            throw new IllegalArgumentException(String.format(WRONG_MODIFIER_SUFFIX_ERROR, modifierSuffix));
        }
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

        Object o = extractFrom(parentObject);
        switch (reduceType) {
            case DO_NOT_REDUCE:
                return o;
            case REDUCE_EVERYTHING:
                MultiResult collector = new MultiResult();
                reduceInto(collector, o);
                return collector;
            case REDUCE_BY_INDEX:
                return getItemAtPositionOrNull(o, (Integer) modifier);
            case REDUCE_BY_MAP_KEY:
                return getFromMapByKey(o, mapKey);
            default:
                throw new IllegalStateException("Getter incorrectly initialized `reduceType` to " + reduceType);
        }
    }

    private Object getFromMapByKey(Object o, String mapKey) {
        if (!(o instanceof Map)) {
            throw new IllegalArgumentException("Can get from object " + o + "only if it's a map for key " + mapKey);
        }
        return ((Map) o).get(mapKey);
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

        if (reduceType == ReduceType.DO_NOT_REDUCE) {
            //We are returning the object as it is.
            //No modifier suffix was defined
            return inputType;
        }

        if (!inputType.isArray()) {
            throw new IllegalArgumentException("Cannot infer a return type with modifier "
                    + modifier + " on type " + inputType.getName());
        }

        //ok, it must be an array. let's return array type
        return inputType.getComponentType();
    }

    private void collectResult(MultiResult collector, Object parentObject)
            throws IllegalAccessException, InvocationTargetException {
        // re-add nulls from parent extraction without extracting further down the path
        if (parentObject == null) {
            collector.add(null);
        } else {
            Object currentObject = extractFrom(parentObject);
            if (shouldReduce()) {
                reduceInto(collector, currentObject);
            } else {
                collector.add(currentObject);
            }
        }
    }

    private Object extractFromMultiResult(MultiResult parentMultiResult) throws IllegalAccessException,
            InvocationTargetException {
        MultiResult collector = new MultiResult();
        collector.setNullOrEmptyTarget(parentMultiResult.isNullEmptyTarget());
        int size = parentMultiResult.getResults().size();
        for (int i = 0; i < size; i++) {
            collectResult(collector, parentMultiResult.getResults().get(i));
        }

        return collector;
    }

    private boolean shouldReduce() {
        return reduceType != ReduceType.DO_NOT_REDUCE;
    }


    private Object getItemAtPositionOrNull(Object object, int position) {
        if (object == null) {
            return null;
        } else if (object instanceof Collection) {
            return CollectionUtil.getItemAtPositionOrNull((Collection) object, position);
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

    private void reduceArrayInto(MultiResult collector, Object[] currentObject) {
        Object[] array = currentObject;
        if (array.length == 0) {
            collector.addNullOrEmptyTarget();
        } else {
            for (int i = 0; i < array.length; i++) {
                collector.add(array[i]);
            }
        }
    }

    private void reducePrimitiveArrayInto(MultiResult collector, Object primitiveArray) {
        int length = Array.getLength(primitiveArray);
        if (length == 0) {
            collector.addNullOrEmptyTarget();
        } else {
            for (int i = 0; i < length; i++) {
                collector.add(Array.get(primitiveArray, i));
            }
        }
    }

    protected void reduceCollectionInto(MultiResult collector, Collection currentObject) {
        Collection collection = currentObject;
        if (collection.isEmpty()) {
            collector.addNullOrEmptyTarget();
        } else {
            for (Object o : collection) {
                collector.add(o);
            }
        }
    }

    protected void reduceInto(MultiResult collector, Object currentObject) {
        if (reduceType == ReduceType.REDUCE_BY_INDEX) {
            Object item = getItemAtPositionOrNull(currentObject, modifier);
            collector.add(item);
            return;
        }
        if (reduceType == ReduceType.REDUCE_BY_MAP_KEY) {
            collector.add(getFromMapByKey(currentObject, mapKey));
            return;
        }

        if (currentObject == null) {
            collector.addNullOrEmptyTarget();
            return;
        }

        reduceAllEntriesInto(collector, currentObject);
    }

    private void reduceAllEntriesInto(MultiResult collector, Object currentObject) {
        if (currentObject instanceof Map) {
            reduceMapInto(collector, (Map) currentObject);
        } else if (currentObject instanceof Collection) {
            reduceCollectionInto(collector, (Collection) currentObject);
        } else if (currentObject instanceof Object[]) {
            reduceArrayInto(collector, (Object[]) currentObject);
        } else if (currentObject.getClass().isArray()) {
            reducePrimitiveArrayInto(collector, currentObject);
        } else {
            throw new IllegalArgumentException("Can't reduce result from a type " + currentObject.getClass()
                    + " Only Collections and Arrays are supported.");
        }
    }

    private void reduceMapInto(MultiResult collector, Map currentObject) {
        for (Object value : currentObject.values()) {
            if (value != null) {
                collector.add(value);
            }
        }
    }

    private static int parseModifier(String modifier) {
        String stringValue = removeBrackets(modifier);
        if (REDUCER_ANY_TOKEN.equals(stringValue)) {
            return MODIFIER_NOT_USED;
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

    private enum ReduceType {
        DO_NOT_REDUCE, REDUCE_EVERYTHING, REDUCE_BY_INDEX, REDUCE_BY_MAP_KEY
    }
}

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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Optional;

import static com.hazelcast.query.impl.getters.AbstractMultiValueGetter.validateModifier;
import static com.hazelcast.query.impl.getters.NullGetter.NULL_GETTER;
import static com.hazelcast.query.impl.getters.NullMultiValueGetter.NULL_MULTIVALUE_GETTER;
import static com.hazelcast.query.impl.predicates.PredicateUtils.unwrapIfOptional;

public final class GetterFactory {

    private static final String ANY = "[any]";

    private GetterFactory() {
    }

    public static Getter newFieldGetter(Object object, Getter parent, Field field, String modifier) throws Exception {
        return newGetter(object, parent, modifier, field.getType(), field::get,
                (t, et) -> new FieldGetter(parent, field, modifier, t, et));
    }

    public static Getter newMethodGetter(Object object, Getter parent, Method method, String modifier) throws Exception {
        return newGetter(object, parent, modifier, method.getReturnType(), o -> method.invoke(o),
                (t, et) -> new MethodGetter(parent, method, modifier, t, et));
    }

    public static Getter newThisGetter(Getter parent, Object object) {
        return new ThisGetter(parent, object);
    }

    private static Getter newGetter(Object object, Getter parent, String modifier, Class type, Reader reader,
                                    Constructor constructor) throws Exception {
        Object currentObject = getCurrentObject(object, parent);
        if (type == Optional.class) {
            type = deduceOptionalType(currentObject, reader);
            if (type == null) {
                return ANY.equals(modifier) ? NULL_MULTIVALUE_GETTER : NULL_GETTER;
            }
        }

        Class elementType = null;
        if (isExtractingFromCollection(type, modifier)) {
            validateModifier(modifier);
            if (currentObject == null) {
                return NULL_GETTER;
            }
            if (currentObject instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) currentObject;
                elementType = deduceElementType(multiResult, reader);
            } else {
                Collection collection = unwrapIfOptional(reader.read(currentObject));
                elementType = deduceElementType(collection);
            }
            if (elementType == null) {
                if (modifier.equals(ANY)) {
                    return NULL_MULTIVALUE_GETTER;
                }
                return NULL_GETTER;
            }
        } else if (isExtractingFromArray(type, modifier)) {
            validateModifier(modifier);
            if (currentObject == null) {
                return NULL_GETTER;
            }
        }
        return constructor.construct(type, elementType);
    }

    private static Class deduceOptionalType(Object object, Reader reader) throws Exception {
        if (object instanceof MultiResult) {
            for (Object result : ((MultiResult) object).getResults()) {
                if (result == null) {
                    continue;
                }
                Class deducedType = deduceOptionalType(reader.read(result));
                if (deducedType != null) {
                    return deducedType;
                }
            }
            return null;
        } else {
            return object == null ? null : deduceOptionalType(reader.read(object));
        }
    }

    private static Class deduceOptionalType(Object value) {
        assert value == null || value instanceof Optional;

        if (value == null) {
            return null;
        }
        Optional optional = (Optional) value;
        return optional.isPresent() ? optional.get().getClass() : null;
    }

    private static Class deduceElementType(MultiResult multiResult, Reader reader) throws Exception {
        for (Object result : multiResult.getResults()) {
            if (result == null) {
                continue;
            }
            Collection collection = unwrapIfOptional(reader.read(result));
            Class elementType = deduceElementType(collection);
            if (elementType != null) {
                return elementType;
            }
        }
        return null;
    }

    private static Class deduceElementType(Collection<?> collection) {
        if (collection == null || collection.isEmpty()) {
            return null;
        }
        Object item = CollectionUtil.getItemAtPositionOrNull(collection, 0);
        if (item == null) {
            for (Object object : collection) {
                if (object != null) {
                    return object.getClass();
                }
            }
            return null;
        }
        return item.getClass();
    }

    private static boolean isExtractingFromCollection(Class type, String modifier) {
        return modifier != null && Collection.class.isAssignableFrom(type);
    }

    private static boolean isExtractingFromArray(Class type, String modifier) {
        return modifier != null && type.isArray();
    }

    private static Object getCurrentObject(Object object, Getter parent) throws Exception {
        return parent == null ? object : parent.getValue(object);
    }

    @FunctionalInterface
    private interface Reader {

        Object read(Object object) throws Exception;

    }

    @FunctionalInterface
    private interface Constructor {

        Getter construct(Class type, Class elementType);

    }

}

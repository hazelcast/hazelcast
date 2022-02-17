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

import java.lang.reflect.Field;

public final class FieldGetter extends AbstractMultiValueGetter {

    private final Field field;

    // for testing purposes only
    public FieldGetter(Getter parent, Field field, String modifier, Class elementType) {
        this(parent, field, modifier, field.getType(), elementType);
    }

    public FieldGetter(Getter parent, Field field, String modifier, Class type, Class elementType) {
        super(parent, modifier, type, elementType);
        this.field = field;
    }

    @Override
    protected Object extractFrom(Object object) throws IllegalAccessException {
        try {
            return field.get(object);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(composeAttributeValueExtractionFailedMessage(field), e);
        }
    }

    @Override
    boolean isCacheable() {
        return true;
    }

    @Override
    public String toString() {
        return "FieldGetter [parent=" + parent + ", field=" + field + ", modifier = " + getModifier() + "]";
    }

}

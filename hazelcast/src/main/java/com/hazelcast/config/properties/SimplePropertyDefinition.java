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

package com.hazelcast.config.properties;

import com.hazelcast.core.TypeConverter;

/**
 * Simple immutable implementation of {@link PropertyDefinition} for convenience of implementors.
 */
public class SimplePropertyDefinition implements PropertyDefinition {

    private final String key;
    private final boolean optional;
    private final TypeConverter typeConverter;
    private final ValueValidator validator;

    public SimplePropertyDefinition(String key, TypeConverter typeConverter) {
        this(key, false, typeConverter, null);
    }

    public SimplePropertyDefinition(String key, boolean optional, TypeConverter typeConverter) {
        this(key, optional, typeConverter, null);
    }

    public SimplePropertyDefinition(String key, boolean optional, TypeConverter typeConverter, ValueValidator validator) {
        this.key = key;
        this.optional = optional;
        this.typeConverter = typeConverter;
        this.validator = validator;
    }

    @Override
    public TypeConverter typeConverter() {
        return typeConverter;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public ValueValidator validator() {
        return validator;
    }

    @Override
    public boolean optional() {
        return optional;
    }
}

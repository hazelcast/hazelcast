/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.type.converter;

/**
 * Converter for {@link java.lang.Character} type.
 */
public final class CharacterConverter extends AbstractStringConverter {

    public static final CharacterConverter INSTANCE = new CharacterConverter();

    private CharacterConverter() {
        super(ID_CHARACTER);
    }

    @Override
    public Class<?> getValueClass() {
        return Character.class;
    }

    @Override
    protected String cast(Object val) {
        Character val0 = (Character) val;

        assert val0 != null;

        return val0.toString();
    }
}

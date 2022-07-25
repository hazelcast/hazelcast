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
package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import java.lang.reflect.Array;

/**
 * Utility methods to be used in compact serialization
 */
public final class CompactUtil {

    private CompactUtil() {

    }

    @Nonnull
    public static HazelcastSerializationException exceptionForUnexpectedNullValue(@Nonnull String fieldName,
                                                                           @Nonnull String methodPrefix,
                                                                           @Nonnull String methodSuffix) {
        return new HazelcastSerializationException("Error while reading " + fieldName + ". "
                + "null value can not be read via " + methodPrefix + methodSuffix + " methods. "
                + "Use " + methodPrefix + "Nullable" + methodSuffix + " instead.");
    }

    @Nonnull
    public static HazelcastSerializationException exceptionForUnexpectedNullValueInArray(@Nonnull String fieldName,
                                                                                  @Nonnull String methodPrefix,
                                                                                  @Nonnull String methodSuffix) {
        return new HazelcastSerializationException("Error while reading " + fieldName + ". "
                + "null value can not be read via " + methodPrefix + "ArrayOf" + methodSuffix + " methods. "
                + "Use " + methodPrefix + "ArrayOfNullable" + methodSuffix + " instead.");
    }

    public static boolean isFieldExist(Schema schema, String fieldName, FieldKind fieldKind) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            return false;
        }

        return field.getKind() == fieldKind;
    }

    public static boolean isFieldExist(Schema schema, String fieldName, FieldKind fieldKind, FieldKind compatibleFieldKind) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            return false;
        }

        return field.getKind() == fieldKind || field.getKind() == compatibleFieldKind;
    }

    public static Enum enumFromStringName(Class<? extends Enum> enumClass, String name) {
        if (name == null) {
            return null;
        }
        return Enum.valueOf(enumClass, name);
    }

    public static String enumAsStringName(Enum e) {
        if (e == null) {
            return null;
        }
        return e.name();
    }

    public static String[] enumArrayAsStringNameArray(Enum[] enumArray) {
        if (enumArray == null) {
            return null;
        }

        String[] stringArray = new String[enumArray.length];
        for (int i = 0; i < enumArray.length; i++) {
            stringArray[i] = enumAsStringName(enumArray[i]);
        }
        return stringArray;
    }

    public static Enum[] enumArrayFromStringNameArray(Class<? extends Enum> enumClass, String[] stringArray) {
        if (stringArray == null) {
            return null;
        }

        Enum[] enumArray = (Enum[]) Array.newInstance(enumClass, stringArray.length);
        for (int i = 0; i < stringArray.length; i++) {
            enumArray[i] = enumFromStringName(enumClass, stringArray[i]);
        }
        return enumArray;
    }

    public static Character characterFromShort(Short s) {
        if (s == null) {
            return null;
        }
        return (char) s.shortValue();
    }

    public static Short characterAsShort(Character c) {
        if (c == null) {
            return null;
        }
        return (short) c.charValue();
    }

    public static char[] charArrayFromShortArray(short[] shortArray) {
        if (shortArray == null) {
            return null;
        }

        char[] charArray = new char[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            charArray[i] = (char) shortArray[i];
        }
        return charArray;
    }

    public static short[] charArrayAsShortArray(char[] charArray) {
        if (charArray == null) {
            return null;
        }

        short[] shortArray = new short[charArray.length];
        for (int i = 0; i < charArray.length; i++) {
            shortArray[i] = (short) charArray[i];
        }
        return shortArray;
    }

    public static Character[] characterArrayFromShortArray(Short[] shortArray) {
        if (shortArray == null) {
            return null;
        }

        Character[] characterArray = new Character[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            characterArray[i] = characterFromShort(shortArray[i]);
        }
        return characterArray;
    }

    public static Short[] characterArrayAsShortArray(Character[] characterArray) {
        if (characterArray == null) {
            return null;
        }

        Short[] shortArray = new Short[characterArray.length];
        for (int i = 0; i < characterArray.length; i++) {
            shortArray[i] = characterAsShort(characterArray[i]);
        }
        return shortArray;
    }
}

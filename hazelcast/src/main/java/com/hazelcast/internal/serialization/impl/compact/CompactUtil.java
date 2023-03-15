/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
    // To determine classes that we won't serialize with zero-config serialization
    private static final UnsupportedPackagePrefix[] UNSUPPORTED_PACKAGE_PREFIXES = new UnsupportedPackagePrefix[]{
            new UnsupportedPackagePrefix("java"),
            new UnsupportedPackagePrefix("javax"),
            new UnsupportedPackagePrefix("com.sun"),
            new UnsupportedPackagePrefix("sun"),
            new UnsupportedPackagePrefix("jdk")
    };

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

    public static void verifyClassIsCompactSerializable(Class<?> clazz) {
        if (canBeSerializedAsCompact(clazz)) {
            return;
        }

        throw new HazelcastSerializationException("The '" + clazz + "' cannot "
                + "be serialized with zero configuration Compact serialization "
                + "because this type is not supported yet. If you want to "
                + "serialize this class, consider writing a CompactSerializer "
                + "for it.");
    }

    public static void verifyFieldClassIsCompactSerializable(Class<?> fieldClass, Class<?> clazz) {
        if (canBeSerializedAsCompact(fieldClass)) {
            return;
        }

        throw new HazelcastSerializationException("The '" + fieldClass + "' "
                + "cannot be serialized with zero configuration Compact "
                + "serialization because this type is not supported yet. If you "
                + "want to serialize '" + clazz + "' which uses this class in "
                + "its fields, consider writing a CompactSerializer for either the '"
                + clazz + "' or the '" + fieldClass + "'.");
    }

    public static void verifyFieldClassShouldBeSerializedAsCompact(CompactStreamSerializer compactStreamSerializer,
                                                                   Class<?> fieldClass, Class<?> clazz) {
        if (compactStreamSerializer.canBeSerializedAsCompact(fieldClass)) {
            return;
        }

        throw new HazelcastSerializationException("The '" + fieldClass + "' "
                + "cannot be serialized with zero configuration Compact "
                + "serialization because this type can be serialized with another "
                + "serialization mechanism. If you want to serialize "
                + "'" + clazz + "' which uses this class in its fields, consider "
                + "overriding that serialization mechanism. You can do that by "
                + "adding '" + fieldClass + "' to CompactSerializationConfig, or "
                + "writing and registering an explicit CompactSerializer for it.");
    }

    private static boolean canBeSerializedAsCompact(Class<?> clazz) {
        Package classPackage = clazz.getPackage();
        if (classPackage == null) {
            // If the Java version is 8, we can hit this branch if the user
            // class does not have a package defined for it (the default
            // package). In Java 9 and above, in that case, we won't hit this,
            // but it will return a package whose name is an empty string.

            // In Java 9 and above, the method returns null if the class is
            // primitive, array or Void.

            // We won't hit this line for primitive types, as they are already
            // supported by the reflective serializers. For arrays, we can only
            // hit this line for arrays of arrays which is not supported by
            // Compact. And, Void is not a type we support in the Compact
            // serialization.

            // So, we can return false here, if the clazz is one of the types
            // mentioned above, and return true if not, to be able to serialize
            // user classes without a package in Java 8.
            return !clazz.isPrimitive() && !clazz.isArray() && !Void.class.equals(clazz);
        }

        String name = classPackage.getName();
        for (UnsupportedPackagePrefix prefix : UNSUPPORTED_PACKAGE_PREFIXES) {
            if (prefix.matches(name)) {
                return false;
            }
        }

        return true;
    }

    /**
     * This class is used to check if the given package name can be supported
     * with the zero-config Compact serialization, by trying to check if the
     * given package name is one of the known JDK package prefixes.
     */
    private static final class UnsupportedPackagePrefix {
        private final String prefix;
        private final String prefixWithDot;

        private UnsupportedPackagePrefix(String prefix) {
            this.prefix = prefix;
            this.prefixWithDot = prefix + ".";
        }

        /**
         * Returns {@code true} if the given package name
         * <ul>
         *     <li>starts with the unsupported {@code prefix + "."}, hence
         *     under some package with the given prefix</li>
         *     <li>equals to the {@code prefix}, hence directly in the
         *     given package</li>
         * </ul>
         * <p>
         * The extra logic is to prevent falsely identifying packages which
         * starts with the given unsupported package name prefix like
         * {@code java_but_not_really_jdk}.
         */
        public boolean matches(String packageName) {
            return packageName.startsWith(prefixWithDot) || packageName.equals(prefix);
        }
    }
}

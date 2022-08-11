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

package com.hazelcast.config;

import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * In 5.0, this feature is disabled by default and has to be enabled via
 * configuration
 * <p>
 * Once enabled the classes which does not match to any other serialization
 * methods will be serialized in the new format automatically via help of
 * reflection without needing any other configuration. For this case, the class
 * should not implement `Serializable`, `Externalizable`, `Portable`,
 * `IdentifiedDataSerializable`, `DataSerializable` or the class should not be
 * registered as a custom serializer.
 * <p>
 * For automatic serialization with reflection, only types in
 * {@link CompactWriter}/{@link CompactReader} interface and {@code char},
 * {@code Character}, {@code enum}, and their arrays are supported as fields.
 * For any other field type, the reflective serializer will work recursively and
 * try to de/serialize those as nested Compact objects.
 * <p>
 * To explicitly configure a class to be serialized with Compact format
 * following methods can be used.
 * <ul>
 *     <li>{@link #addSerializer(CompactSerializer)}</li>
 *     <li>{@link #setSerializers(CompactSerializer[])}</li>
 *     <li>{@link #addClass(Class)}</li>
 *     <li>{@link #setClasses(Class[])}</li>
 * </ul>
 * <p>
 * On the first two methods listed above, reflection is not utilized. Instead,
 * the given serializer is used to serialize/deserialize users objects.
 *
 * @since Hazelcast 5.0 as BETA. The final version will not be backward
 * compatible with the Beta. Do not use BETA version in production
 */
@Beta
public class CompactSerializationConfig {

    // Package-private to access those via CompactSerializationConfigAccessor
    final Map<String, TriTuple<Class, String, CompactSerializer>> typeNameToRegistration;
    final Map<Class, TriTuple<Class, String, CompactSerializer>> classToRegistration;
    final List<String> serializerClassNames;
    final List<String> compactSerializableClassNames;

    private boolean enabled;

    public CompactSerializationConfig() {
        this.typeNameToRegistration = new ConcurrentHashMap<>();
        this.classToRegistration = new ConcurrentHashMap<>();
        this.serializerClassNames = new CopyOnWriteArrayList<>();
        this.compactSerializableClassNames = new CopyOnWriteArrayList<>();
    }

    public CompactSerializationConfig(CompactSerializationConfig config) {
        this.typeNameToRegistration = new ConcurrentHashMap<>(config.typeNameToRegistration);
        this.classToRegistration = new ConcurrentHashMap<>(config.classToRegistration);
        this.serializerClassNames = new CopyOnWriteArrayList<>(config.serializerClassNames);
        this.compactSerializableClassNames = new CopyOnWriteArrayList<>(config.compactSerializableClassNames);
        this.enabled = config.enabled;
    }

    /**
     * Registers the class to be serialized with Compact serialization.
     * <p>
     * For the given class, Compact serialization will be used instead of
     * Portable, Identified, Java Serializable, or GlobalSerializer.
     * <p>
     * Type name is determined automatically from the class, which is its fully
     * qualified class name.
     * <p>
     * Field types are determined automatically from the class with reflection.
     *
     * @param clazz Class to be serialized with Compact serialization
     * @return configured {@link CompactSerializationConfig} for chaining
     */
    public <T> CompactSerializationConfig addClass(@Nonnull Class<T> clazz) {
        checkNotNull(clazz, "Class cannot be null");
        register0(clazz, clazz.getName(), null);
        return this;
    }

    /**
     * Registers multiple classes to be serialized with Compact serialization.
     * <p>
     * For the given classes, Compact serialization will be used instead of
     * Portable, Identified, Java Serializable, or GlobalSerializer.
     * <p>
     * Type names are determined automatically from the classes, which are their
     * fully qualified class names.
     * <p>
     * Field types are determined automatically from the classes with
     * reflection.
     * <p>
     * This method will clear previous class registrations.
     *
     * @param classes Classes to be serialized with Compact serialization
     * @return configured {@link CompactSerializationConfig} for chaining
     */
    public CompactSerializationConfig setClasses(@Nonnull Class<?>... classes) {
        clearClassRegistrations();
        for (Class<?> clazz : classes) {
            addClass(clazz);
        }
        return this;
    }

    /**
     * Registers the given Compact serializer.
     * <p>
     * For the class returned by the serializer's
     * {@link CompactSerializer#getCompactClass()} method, Compact serialization
     * will be used instead of Portable, Identified, Java Serializable, or
     * GlobalSerializer.
     * <p>
     * Type name will be read from the serializer's
     * {@link CompactSerializer#getTypeName()} method.
     *
     * @param serializer Serializer to be registered
     * @return configured {@link CompactSerializationConfig} for chaining
     */
    public <T> CompactSerializationConfig addSerializer(@Nonnull CompactSerializer<T> serializer) {
        checkNotNull(serializer, "Serializer cannot be null");
        register0(serializer.getCompactClass(), serializer.getTypeName(), serializer);
        return this;
    }

    /**
     * Registers the given Compact serializers.
     * <p>
     * For the classes returned by the serializers'
     * {@link CompactSerializer#getCompactClass()} method, Compact serialization
     * will be used instead of Portable, Identified, Java Serializable, or
     * GlobalSerializer.
     * <p>
     * Type names will be read from the serializers'
     * {@link CompactSerializer#getTypeName()} method.
     * <p>
     * This method will clear previous serializer registrations.
     *
     * @param serializers Serializers to be registered
     * @return configured {@link CompactSerializationConfig} for chaining
     */
    public CompactSerializationConfig setSerializers(@Nonnull CompactSerializer<?>... serializers) {
        clearSerializerRegistrations();
        for (CompactSerializer<?> serializer : serializers) {
            addSerializer(serializer);
        }
        return this;
    }

    /**
     * This method will only be available during @Beta.
     *
     * @return true if compact serialization is enable.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables the Compact Format. The Compact Format will be disabled by
     * default during the Beta period. It will be enabled by default after the
     * Beta. Note that this method will be deleted after the Beta.
     * <p>
     * The final version will not be backward compatible with the Beta. Do not
     * use BETA version in production
     *
     * @param enabled Enables the Compact Format when set to true
     * @return configured {@link CompactSerializationConfig} for chaining
     */
    public CompactSerializationConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    private void register0(Class clazz, String typeName, CompactSerializer explicitSerializer) {
        TriTuple<Class, String, CompactSerializer> registration = TriTuple.of(clazz, typeName, explicitSerializer);
        TriTuple<Class, String, CompactSerializer> oldRegistration = typeNameToRegistration.putIfAbsent(typeName, registration);
        if (oldRegistration != null) {
            throw new InvalidConfigurationException("Duplicate serializer registrations "
                    + "are found for the type name '" + typeName + "'. Make sure only one "
                    + "Compact serializer is registered for the same type name. "
                    + "Existing serializer: " + oldRegistration.element3 + ", "
                    + "new serializer: " + registration.element3);
        }
        oldRegistration = classToRegistration.putIfAbsent(clazz, registration);
        if (oldRegistration != null) {
            throw new InvalidConfigurationException("Duplicate serializer registrations "
                    + "are found for the class '" + clazz + "'. Make sure only one "
                    + "Compact serializer is registered for the same class. "
                    + "Existing serializer: " + oldRegistration.element3 + ", "
                    + "new serializer: " + registration.element3);
        }
    }

    private void clearClassRegistrations() {
        typeNameToRegistration.entrySet().removeIf(entry -> entry.getValue().element3 == null);
        classToRegistration.entrySet().removeIf(entry -> entry.getValue().element3 == null);
    }

    private void clearSerializerRegistrations() {
        typeNameToRegistration.entrySet().removeIf(entry -> entry.getValue().element3 != null);
        classToRegistration.entrySet().removeIf(entry -> entry.getValue().element3 != null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactSerializationConfig that = (CompactSerializationConfig) o;
        return enabled == that.enabled
                && Objects.equals(typeNameToRegistration, that.typeNameToRegistration)
                && Objects.equals(classToRegistration, that.classToRegistration)
                && Objects.equals(serializerClassNames, that.serializerClassNames)
                && Objects.equals(compactSerializableClassNames, that.compactSerializableClassNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, typeNameToRegistration, classToRegistration,
                serializerClassNames, compactSerializableClassNames);
    }
}

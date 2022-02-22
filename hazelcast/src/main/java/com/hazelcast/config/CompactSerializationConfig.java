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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * In 5.0, this feature is disabled by default and has to be enabled via configuration
 * <p>
 * Once enabled the classes which does not match to any other serialization methods will be serialized in
 * the new format automatically via help of reflection without needing any other configuration.
 * For this case, the class should not implement `Serializable`, `Externalizable` , `Portable` ,
 * `IdentifiedDataSerializable`, `DataSerializable` or the class should not be registered as a custom serializer.
 * <p>
 * Automatic serialization via reflection can de/serialize classes having an accessible empty constructor only.
 * Only types in {@link CompactWriter}/{@link CompactReader} interface are supported as fields.
 * For any other class as the field type, it will work recursively and try to de/serialize a sub-class.
 * Thus, if any sub-fields does not have an accessible empty constructor, deserialization fails with
 * HazelcastSerializationException.
 * <p>
 * To explicitly configure a class to be serialized via Compact format following methods can be used.
 * {@link #register(Class)}
 * {@link #register(Class, String, CompactSerializer)} }
 * <p>
 * On the last two methods listed above reflection is not utilized instead given serializer is used to
 * serialize/deserialize users objects.
 *
 * @since Hazelcast 5.0 as BETA. The final version will not be backward compatible with the Beta.
 * Do not use BETA version in production
 */
@Beta
public class CompactSerializationConfig {

    // Package-private to access those via CompactSerializationConfigAccessor
    final Map<String, TriTuple<String, String, String>> typeNameToNamedRegistration;
    final Map<String, TriTuple<String, String, String>> classNameToNamedRegistration;
    final Map<String, TriTuple<Class, String, CompactSerializer>> typeNameToRegistration;
    final Map<Class, TriTuple<Class, String, CompactSerializer>> classToRegistration;

    private boolean enabled;

    public CompactSerializationConfig() {
        this.typeNameToRegistration = new ConcurrentHashMap<>();
        this.classToRegistration = new ConcurrentHashMap<>();
        this.typeNameToNamedRegistration = new ConcurrentHashMap<>();
        this.classNameToNamedRegistration = new ConcurrentHashMap<>();
    }

    public CompactSerializationConfig(CompactSerializationConfig compactSerializationConfig) {
        this.typeNameToRegistration = new ConcurrentHashMap<>(compactSerializationConfig.typeNameToRegistration);
        this.classToRegistration = new ConcurrentHashMap<>(compactSerializationConfig.classToRegistration);
        this.typeNameToNamedRegistration = new ConcurrentHashMap<>(compactSerializationConfig.typeNameToNamedRegistration);
        this.classNameToNamedRegistration = new ConcurrentHashMap<>(compactSerializationConfig.classNameToNamedRegistration);
        this.enabled = compactSerializationConfig.enabled;
    }

    /**
     * Registers the class to be serialized via compact serializer.
     * <p>
     * Overrides Portable, Identified, Java Serializable, or GlobalSerializer.
     * <p>
     * Type name is determined automatically from the class, which is its
     * fully qualified class name.
     * <p>
     * Field types are determined automatically from the class via reflection.
     *
     * @param clazz Class to be serialized via compact serializer
     * @return configured {@link com.hazelcast.config.CompactSerializationConfig} for chaining
     */
    public <T> CompactSerializationConfig register(@Nonnull Class<T> clazz) {
        checkNotNull(clazz, "Class cannot be null");
        register0(clazz, clazz.getName(), null);
        return this;
    }

    /**
     * Registers the class to be serialized via compact serializer.
     * <p>
     * Overrides Portable, Identified, Java Serializable, or GlobalSerializer.
     *
     * @param clazz              Class to be serialized via compact serializer
     * @param typeName           Type name of the class
     * @param explicitSerializer Serializer to be used for the given class
     * @return configured {@link com.hazelcast.config.CompactSerializationConfig} for chaining
     */
    public <T> CompactSerializationConfig register(@Nonnull Class<T> clazz, @Nonnull String typeName,
                                                   @Nonnull CompactSerializer<T> explicitSerializer) {
        checkNotNull(clazz, "Class cannot be null");
        checkNotNull(typeName, "Type name cannot be null");
        checkNotNull(explicitSerializer, "Explicit serializer cannot be null");
        register0(clazz, typeName, explicitSerializer);
        return this;
    }

    private <T> void register0(Class<T> clazz, String typeName, CompactSerializer<T> explicitSerializer) {
        TriTuple<Class, String, CompactSerializer> registration = TriTuple.of(clazz, typeName, explicitSerializer);
        TriTuple<Class, String, CompactSerializer> oldRegistration = typeNameToRegistration.putIfAbsent(typeName, registration);
        if (oldRegistration != null) {
            throw new InvalidConfigurationException("Already have a registration for the type name " + typeName);
        }
        oldRegistration = classToRegistration.putIfAbsent(clazz, registration);
        if (oldRegistration != null) {
            throw new InvalidConfigurationException("Already have a registration for class " + clazz);
        }
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
     * Enables the Compact Format. The Compact Format will be disabled by default during the Beta period.
     * It will be enabled by default after the Beta.
     * Note that this method will be deleted after the Beta.
     * <p>
     * The final version will not be backward compatible with the Beta.
     * Do not use BETA version in production
     *
     * @param enabled Enables the Compact Format when set to true
     * @return configured {@link com.hazelcast.config.CompactSerializationConfig} for chaining
     */
    public CompactSerializationConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
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
                && Objects.equals(typeNameToNamedRegistration, that.typeNameToNamedRegistration)
                && Objects.equals(classNameToNamedRegistration, that.classNameToNamedRegistration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, typeNameToRegistration, classToRegistration,
                typeNameToNamedRegistration, classNameToNamedRegistration);
    }
}

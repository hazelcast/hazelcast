/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Handles deserialization of generic objects provided by users. For example, user may specify {@code implementation}
 * field to custom object, then it is serialized and deserialized on other member. If that member does not contain given class,
 * a very cryptic/uninformative exception is thrown. This utility wraps it, giving more context.
 */
@PrivateApi
public final class ConfigDeserializationUtil {

    private ConfigDeserializationUtil() {
    }

    /**
     * Reads object from given {@link ObjectDataInput} and returns customized error message if
     * an {@link HazelcastSerializationException} occurs or the returned object is a {@link GenericRecord},
     * meaning the requested class was not found on the member.
     */
    @Nullable
    static <T> T readObject(@Nonnull ObjectDataInput in, Class<?> configurationClass, @Nonnull String configField)
        throws IOException {

        T objectRead;
        try {
            objectRead = in.readObject();
        } catch (HazelcastSerializationException e) {
            throw new HazelcastSerializationException(buildErrorMessage(configurationClass, configField), e);
        }

        if (objectRead instanceof GenericRecord) {
            String hint = ", looks like the class is missing on the classpath on this member";
            throw new HazelcastSerializationException(buildErrorMessage(configurationClass, configField) + hint);
        }
        return objectRead;
    }

    private static String buildErrorMessage(Class<?> configurationClass, String fieldName) {
        String caller = configurationClass.getSimpleName();
        return "Unable to deserialize %s's %s".formatted(caller, fieldName);
    }
}

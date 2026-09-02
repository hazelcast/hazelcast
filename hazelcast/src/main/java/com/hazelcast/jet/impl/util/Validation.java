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
package com.hazelcast.jet.impl.util;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple utility to validate many checks at once, not interrupting after first error.
 */
@SuppressWarnings("UnusedReturnValue")
public class Validation {
    private final List<String> errors = new ArrayList<>();

    private Validation() {
    }

    @Nonnull
    public static Validation validate() {
        return new Validation();
    }

    @Nonnull
    public Validation checkNotNull(Object field, String fieldName) {
        if (field == null) {
            errors.add(String.format("%s must not be null", fieldName));
        }
        return this;
    }

    /**
     * @see Util#checkSerializable(Object, String)
     */
    @Nonnull
    public Validation checkNotNullAndSerializable(Object object, String fieldName) {
        if (object == null) {
            errors.add(String.format("%s must not be null", fieldName));
        }
        try {
            Util.checkSerializable(object, fieldName);
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }
        return this;
    }

    /**
     * @see Util#checkSerializable(Object, String)
     */
    @Nonnull
    public Validation checkSerializable(Object object, String fieldName) {
        try {
            Util.checkSerializable(object, fieldName);
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }
        return this;
    }

    /**
     * @see Util#checkSerializable(Object, String)
     */
    @Nonnull
    public Validation checkSerializableIfNotNull(Object object, String fieldName) {
        if (object == null) {
            return this;
        }
        try {
            Util.checkSerializable(object, fieldName);
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }
        return this;
    }

    /**
     * If check is false, adds given error to the list.
     */
    @Nonnull
    public Validation check(boolean check, String errorMessage) {
        if (!check) {
            errors.add(errorMessage);
        }
        return this;
    }


    /**
     * Adds given error report to the list.
     */
    @Nonnull
    public Validation reportError(String error) {
        errors.add(error);
        return this;
    }

    /**
     * Throws {@link IllegalArgumentException} if any violations were spotted.
     */
    public void throwIfErrors() {
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errors));
        }
    }
}

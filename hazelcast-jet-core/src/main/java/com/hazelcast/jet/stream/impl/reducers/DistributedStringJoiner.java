/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.reducers;

import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * {@code Serializable} variant of {@link StringJoiner java.util.StringJoiner}.
 */
public final class DistributedStringJoiner implements Serializable {
    private final String prefix;
    private final String delimiter;
    private final String suffix;

    private StringBuilder value;

    private String emptyValue;

    public DistributedStringJoiner(CharSequence delimiter) {
        this(delimiter, "", "");
    }

    public DistributedStringJoiner(CharSequence delimiter, CharSequence prefix, CharSequence suffix) {
        Objects.requireNonNull(prefix, "The prefix must not be null");
        Objects.requireNonNull(delimiter, "The delimiter must not be null");
        Objects.requireNonNull(suffix, "The suffix must not be null");
        // make defensive copies of arguments
        this.prefix = prefix.toString();
        this.delimiter = delimiter.toString();
        this.suffix = suffix.toString();
        this.emptyValue = this.prefix + this.suffix;
    }

    public DistributedStringJoiner setEmptyValue(CharSequence emptyValue) {
        this.emptyValue = Objects.requireNonNull(emptyValue,
                "The empty value must not be null").toString();
        return this;
    }

    @Override
    public String toString() {
        if (value == null) {
            return emptyValue;
        } else {
            if (suffix.isEmpty()) {
                return value.toString();
            } else {
                int initialLength = value.length();
                String result = value.append(suffix).toString();
                // reset value to pre-append initialLength
                value.setLength(initialLength);
                return result;
            }
        }
    }

    public DistributedStringJoiner add(CharSequence newElement) {
        prepareBuilder().append(newElement);
        return this;
    }

    public DistributedStringJoiner merge(DistributedStringJoiner other) {
        Objects.requireNonNull(other);
        if (other.value != null) {
            prepareBuilder().append(other.value, other.prefix.length(), other.value.length());
        }
        return this;
    }

    private StringBuilder prepareBuilder() {
        if (value != null) {
            value.append(delimiter);
        } else {
            value = new StringBuilder().append(prefix);
        }
        return value;
    }

    public int length() {
        // Remember that we never actually append the suffix unless we return
        // the full (present) value or some sub-string or length of it, so that
        // we can add on more if we need to.
        return (value != null ? value.length() + suffix.length()
                : emptyValue.length());
    }
}

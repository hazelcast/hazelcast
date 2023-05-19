/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.connector.mongodb;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Placeholder for a reference to a field from the input row.
 */
public class InputRef implements DynamicallyReplacedPlaceholder {

    private static final Pattern PATTERN = Pattern.compile("<!InputRef\\((\\d+)\\)!>");

    private final int inputIndex;

    public InputRef(int inputIndex) {
        this.inputIndex = inputIndex;
    }

    public int getInputIndex() {
        return inputIndex;
    }

    @Nonnull
    public String asString() {
        return "<!InputRef(" + inputIndex + ")!>";
    }

    public static InputRef match(Object o) {
        if (o instanceof String) {
            Matcher matcher = PATTERN.matcher((String) o);
            if (matcher.matches()) {
                return new InputRef(Integer.parseInt(matcher.group(1)));
            }
        }

        return null;
    }
    public static boolean matches(Object o) {
        if (o instanceof String) {
            Matcher matcher = PATTERN.matcher((String) o);
            return matcher.matches();
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InputRef)) {
            return false;
        }
        InputRef inputRef = (InputRef) o;
        return inputIndex == inputRef.inputIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputIndex);
    }

    @Override
    public String toString() {
        return "InputRef(" + inputIndex + ')';
    }
}

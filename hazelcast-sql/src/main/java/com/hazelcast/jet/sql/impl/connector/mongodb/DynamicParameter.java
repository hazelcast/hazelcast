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
 * Class used to mark a place in Mongo filter/projection, that will be replaced with dynamic parameter.
 *
 * Serialized form is a string matching {@linkplain #PATTERN}.
 */
public class DynamicParameter implements DynamicallyReplacedPlaceholder {

    private static final Pattern PATTERN = Pattern.compile("<!DynamicParameter\\((\\d+)\\)!>");
    private final int index;

    public DynamicParameter(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Nonnull
    @Override
    public String asString() {
        return "<!DynamicParameter(" + index + ")!>";
    }

    public static DynamicParameter matches(Object o) {
        if (o instanceof String) {
            Matcher matcher = PATTERN.matcher((String) o);
            if (matcher.matches()) {
                return new DynamicParameter(Integer.parseInt(matcher.group(1)));
            }
        }

        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DynamicParameter)) {
            return false;
        }
        DynamicParameter that = (DynamicParameter) o;
        return index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    @Override
    public String toString() {
        return "DynamicParameter(" + index + ')';
    }
}

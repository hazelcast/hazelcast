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

import org.bson.Document;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Class used to mark a place in Mongo filter/projection, that will be replaced with dynamic parameter.
 *
 * Serialized form looks like:
 * <pre>{@code
 * "someParam": {
 *     "objectType": "DynamicParameter",
 *     "index": 1
 * }
 * }</pre>
 */
public class DynamicParameter implements Serializable {

    private static final List<String> NODES = asList("index", "objectType");
    private static final String DYNAMIC_PARAMETER_DISCRIMINATOR = "DynamicParameter";

    private final String objectType = DYNAMIC_PARAMETER_DISCRIMINATOR;
    private final int index;

    public DynamicParameter(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    /**
     * Always {@linkplain #DYNAMIC_PARAMETER_DISCRIMINATOR}.
     */
    public String getObjectType() {
        return objectType;
    }

    /**
     * Parses given document and check if it follows the structure of this marking object:
     * <ul>
     *     <li>Two keys</li>
     *     <li>Keys have name as in {@linkplain #NODES}</li>
     *     <li>objectType key has value {@linkplain #DYNAMIC_PARAMETER_DISCRIMINATOR}</li>
     *     <li>Index key has integer value</li>
     * </ul>
     *
     * If all the critera are met, it will return a new {@linkplain DynamicParameter} object
     * with {@linkplain #getIndex()} equal to the value of {@code index} key.
     *
     *  Returns {@code null} if given document does not follow the required structure.
     */
    public static DynamicParameter parse(Document doc) {
        Set<String> keySet = doc.keySet();
        if (keySet.size() == 2 && keySet.containsAll(NODES)) {
            Object objectType = doc.get("objectType");
            assert objectType instanceof String;
            if (DYNAMIC_PARAMETER_DISCRIMINATOR.equals(objectType)) {
                return new DynamicParameter(doc.getInteger("index"));
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
        return Objects.hash(objectType, index);
    }

    @Override
    public String toString() {
        return "ParameterReplace(" + index + ')';
    }
}

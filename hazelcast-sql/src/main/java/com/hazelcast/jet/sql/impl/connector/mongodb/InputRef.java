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
 * Reference to some input value.
 */
public class InputRef implements Serializable {

    private static final List<String> NODES = asList("inputIndex", "objectType");
    private static final String INPUT_REF_DISCRIMINATOR = "DynamicParameter";

    private final int inputIndex;

    private final String objectType = INPUT_REF_DISCRIMINATOR;

    public InputRef(int inputIndex) {
        this.inputIndex = inputIndex;
    }

    public int getInputIndex() {
        return inputIndex;
    }

    /**
     * Returns object type of InputRef, always {@link #INPUT_REF_DISCRIMINATOR}.
     */
    public String getObjectType() {
        return objectType;
    }

    /**
     * Parses given document and check if it follows the structure of this marking object:
     * <ul>
     *     <li>Two keys</li>
     *     <li>Keys have name as in {@linkplain #NODES}</li>
     *     <li>objectType key has value {@linkplain #INPUT_REF_DISCRIMINATOR}</li>
     *     <li>Index key has integer value</li>
     * </ul>
     *
     * If all the critera are met, it will return a new {@linkplain InputRef} object
     * with {@linkplain #getInputIndex()} equal to the value of {@code index} key.
     *
     *  Returns {@code null} if given document does not follow the required structure.
     */
    public static InputRef parse(Document doc) {
        Set<String> keySet = doc.keySet();
        if (keySet.size() == 2 && keySet.containsAll(NODES)) {
            Object objectType = doc.get("objectType");
            assert objectType instanceof String;
            if (INPUT_REF_DISCRIMINATOR.equals(objectType)) {
                return new InputRef(doc.getInteger("inputIndex"));
            }
        }
        return null;
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
        return Objects.hash(inputIndex, objectType);
    }

    @Override
    public String toString() {
        return "InputRef(" + inputIndex + ')';
    }
}

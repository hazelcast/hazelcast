/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.projection.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.AttributeOrigin;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

import static com.hazelcast.query.impl.MapEntryAttributeExtractor.extractAttributeValue;
import static com.hazelcast.query.impl.predicates.PredicateUtils.canonicalizeAttribute;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkHasText;

/**
 * Projection that extracts the values of the given attributes and returns them in an Object[] array.
 * <p>
 * The attributePath does not support the [any] operator.
 * The input object has to be an instance of Extractable in order for the projection to work.
 *
 * @param <I> type of the input
 */
@SuppressFBWarnings("SE_NO_SERIALVERSIONID")
public final class MultiAttributeProjection<I> extends Projection<I, Object[]> implements IdentifiedDataSerializable {

    private String[] attributePaths;

    private transient AttributeOrigin[] attributeOrigins;

    MultiAttributeProjection() {
    }

    public MultiAttributeProjection(String... attributePath) {
        if (attributePath == null || attributePath.length == 0) {
            throw new IllegalArgumentException("You need to specify at least one attributePath");
        }
        for (String path : attributePath) {
            checkHasText(path, "attributePath must not be null or empty");
            checkFalse(path.contains("[any]"), "attributePath must not contain [any] operators");
        }
        this.attributePaths = attributePath;
        for (int i = 0; i < this.attributePaths.length; i++) {
            this.attributePaths[i] = canonicalizeAttribute(attributePaths[i]);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] transform(I input) {
        if (input instanceof QueryableEntry) {
            Object[] result = new Object[attributePaths.length];
            for (int i = 0; i < attributePaths.length; i++) {
                result[i] = extractAttributeValue((QueryableEntry) input, attributePaths[i], getAttributeOrigin(i));
            }
            return result;
        }
        throw new IllegalArgumentException("The given map entry is not extractable");
    }

    @Override
    public int getFactoryId() {
        return ProjectionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ProjectionDataSerializerHook.MULTI_ATTRIBUTE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTFArray(attributePaths);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePaths = in.readUTFArray();
    }

    private AttributeOrigin getAttributeOrigin(int index) {
        if (attributeOrigins == null) {
            attributeOrigins = new AttributeOrigin[this.attributePaths.length];
        }
        if (attributeOrigins[index] == null) {
            attributeOrigins[index] = AttributeOrigin.fromName(attributePaths[index]);
        }
        return attributeOrigins[index];
    }

}

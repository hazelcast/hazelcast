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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Comparables;
import com.hazelcast.query.impl.Indexes;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.query.impl.predicates.PredicateUtils.isNull;

/**
 * Not Equal Predicate
 */
@BinaryInterface
public class NotEqualPredicate extends AbstractPredicate implements NegatablePredicate, VisitablePredicate {

    private static final long serialVersionUID = 1L;

    Comparable value;

    public NotEqualPredicate() {
    }

    public NotEqualPredicate(String attribute, Comparable value) {
        super(attribute);
        this.value = value;
    }

    public Comparable getValue() {
        return value;
    }

    @Override
    public Predicate accept(Visitor visitor, Indexes indexes) {
        return visitor.visit(this, indexes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean apply(Map.Entry mapEntry) {
        return !super.apply(mapEntry);
    }

    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
        // XXX: The code below performs equality check, instead of inequality.
        // The result of this check is negated in NotEqualPredicate.apply method.
        // This is required to make multi-value attribute inequality queries to
        // work properly: if something has two names A and B, that something
        // should be excluded if we are searching for things not named A, even
        // if its another name is B.

        if (attributeValue == null) {
            return isNull(value);
        }
        value = convert(attributeValue, value);
        attributeValue = (Comparable) convertEnumValue(attributeValue);
        return Comparables.equal(attributeValue, value);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof NotEqualPredicate)) {
            return false;
        }

        NotEqualPredicate that = (NotEqualPredicate) o;
        if (!that.canEqual(this)) {
            return false;
        }

        return Objects.equals(value, that.value);
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof NotEqualPredicate;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return attributeName + " != " + value;
    }

    @Override
    public Predicate negate() {
        return new EqualPredicate(attributeName, value);
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.NOTEQUAL_PREDICATE;
    }

}

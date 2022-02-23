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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;

/**
 * Not Predicate
 */
@BinaryInterface
public final class NotPredicate
        implements Predicate, VisitablePredicate, NegatablePredicate, IdentifiedDataSerializable {

    private static final long serialVersionUID = 1L;

    protected Predicate predicate;

    public NotPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    public NotPredicate() {
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return !predicate.apply(mapEntry);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
    }

    @Override
    public String toString() {
        return "NOT(" + predicate + ")";
    }

    @Override
    public Predicate accept(Visitor visitor, Indexes indexes) {
        Predicate target = predicate;
        if (predicate instanceof VisitablePredicate) {
            target = ((VisitablePredicate) predicate).accept(visitor, indexes);
        }
        if (target == predicate) {
            // visitor didn't change the inner predicate
            return visitor.visit(this, indexes);
        }

        // visitor returned a different copy of the inner predicate.
        // We have to create our copy with the new inner predicate to maintained immutability
        NotPredicate copy = new NotPredicate(target);
        return visitor.visit(copy, indexes);
    }

    @Override
    public Predicate negate() {
        return predicate;
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.NOT_PREDICATE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof NotPredicate)) {
            return false;
        }

        NotPredicate that = (NotPredicate) o;
        return predicate != null ? predicate.equals(that.predicate) : that.predicate == null;
    }

    @Override
    public int hashCode() {
        return predicate != null ? predicate.hashCode() : 0;
    }
}

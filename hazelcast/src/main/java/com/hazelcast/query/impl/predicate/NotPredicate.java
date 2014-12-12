/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Map;

/**
 * Not Predicate
 */
public class NotPredicate implements Predicate, DataSerializable {
    private Predicate predicate;

    public NotPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    public NotPredicate() {
    }

    @Override
    public boolean equals(Object predicate) {
        return predicate.equals(this);
    }

    public int hashCode() {
        return predicate.hashCode();
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return !predicate.apply(mapEntry);
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        return this.predicate.isSubSet(predicate);
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
}

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

package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.BinaryInterface;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class contains server side implementation of PredicateBuilder.
 */
@BinaryInterface
public class AbstractPredicateBuilder implements IndexAwarePredicate {

    protected List<Predicate> lsPredicates = new ArrayList<Predicate>();
    protected String attribute;

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return lsPredicates.get(0).apply(mapEntry);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Predicate p = lsPredicates.get(0);
        if (p instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) p).filter(queryContext);
        }
        return null;
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        Predicate p = lsPredicates.get(0);
        if (p instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) p).isIndexed(queryContext);
        }
        return false;
    }

    @Override
    public String toString() {
        return "PredicateBuilder{\n" + (lsPredicates.size() == 0 ? "" : lsPredicates.get(0)) + "\n}";
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attribute);
        out.writeInt(lsPredicates.size());
        for (Predicate predicate : lsPredicates) {
            out.writeObject(predicate);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readUTF();
        int size = in.readInt();
        lsPredicates = new ArrayList<Predicate>(size);
        for (int i = 0; i < size; i++) {
            lsPredicates.add((Predicate) in.readObject());
        }
    }
}

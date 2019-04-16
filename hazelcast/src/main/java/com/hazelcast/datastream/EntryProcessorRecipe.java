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

package com.hazelcast.datastream;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;

/**
 *
 */
public class EntryProcessorRecipe implements DataSerializable {

    private Mutator mutator;
    private Predicate predicate;

    public EntryProcessorRecipe() {
    }

    public EntryProcessorRecipe(Predicate predicate, Mutator mutator) {
        this.predicate = predicate;
        this.mutator = mutator;
    }

    public Mutator getMutator() {
        return mutator;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(predicate);
        out.writeObject(mutator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
        mutator = in.readObject();
    }
}

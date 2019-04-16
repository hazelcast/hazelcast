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
 * A recipe for creating a projection using the {@link DataStream}.
 *
 * @param <E>
 */
public class ProjectionRecipe<E> implements DataSerializable {

    private String className;
    private boolean reusePojo;
    private Predicate predicate;

    public ProjectionRecipe() {
    }

    public ProjectionRecipe(Class<E> clazz, boolean reusePojo, Predicate predicate) {
        this(clazz.getName(), reusePojo, predicate);
    }

    public ProjectionRecipe(String className, boolean reusePojo, Predicate predicate) {
        this.className = className;
        this.reusePojo = reusePojo;
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public String getClassName() {
        return className;
    }

    public boolean isReusePojo() {
        return reusePojo;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(className);
        out.writeBoolean(reusePojo);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readUTF();
        reusePojo = in.readBoolean();
        predicate = in.readObject();
    }
}

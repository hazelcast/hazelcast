/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Map;

/**
 * Predicate version of `instaceof` operator from Java.
 *
 */
public class InstanceOfPredicate implements Predicate, DataSerializable {
    private Class klass;

    public InstanceOfPredicate(Class klass) {
        this.klass = klass;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        Object value = mapEntry.getValue();
        if (value == null) {
            return false;
        }
        return klass.isAssignableFrom(value.getClass());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(klass.getName());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        String klassName = in.readUTF();
        try {
            klass = in.getClassLoader().loadClass(klassName);
        } catch (ClassNotFoundException e) {
            throw new HazelcastSerializationException("Failed to load class: " + klass, e);
        }
    }

    @Override
    public String toString() {
        return " instanceOf (" + klass.getName() + ")";
    }
}

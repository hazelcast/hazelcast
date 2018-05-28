/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;

/**
 * Predicate version of `instaceof` operator from Java.
 *
 */
@BinaryInterface
public class InstanceOfPredicate
        implements Predicate, IdentifiedDataSerializable {
    private Class klass;

    public InstanceOfPredicate(Class klass) {
        this.klass = klass;
    }

    public InstanceOfPredicate() {
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
            klass = ClassLoaderUtil.loadClass(in.getClassLoader(), klassName);
        } catch (ClassNotFoundException e) {
            throw new HazelcastSerializationException("Failed to load class: " + klass, e);
        }
    }

    @Override
    public String toString() {
        return " instanceOf (" + klass.getName() + ")";
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.INSTANCEOF_PREDICATE;
    }
}

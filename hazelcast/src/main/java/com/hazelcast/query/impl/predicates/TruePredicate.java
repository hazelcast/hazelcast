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

import java.io.IOException;
import java.util.Map;

/**
 * A {@link com.hazelcast.query.Predicate} which always returns true.
 *
 * @param <K> map key type
 * @param <V> map value type
 */
@BinaryInterface
public class TruePredicate<K, V> implements IdentifiedDataSerializable, Predicate<K, V> {

    /**
     * An instance of the TruePredicate.
     */
    public static final TruePredicate INSTANCE = new TruePredicate();

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    public static <K, V> TruePredicate<K, V> truePredicate() {
        return INSTANCE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return true;
    }

    @Override
    public String toString() {
        return "TruePredicate{}";
    }

    @Override
    public int getFactoryId() {
        return PredicateDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.TRUE_PREDICATE;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TruePredicate;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

}

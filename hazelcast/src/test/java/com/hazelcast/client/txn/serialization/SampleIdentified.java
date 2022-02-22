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

package com.hazelcast.client.txn.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

class SampleIdentified implements IdentifiedDataSerializable {

    public static final int FACTORY_ID = 1;
    public static final int CLASS_ID = 1;

    private int amount;

    SampleIdentified() {
    }

    SampleIdentified(int amount) {
        this.amount = amount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(amount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        amount = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SampleIdentified)) {
            return false;
        }

        SampleIdentified that = (SampleIdentified) o;

        return amount == that.amount;
    }

    @Override
    public int hashCode() {
        return amount;
    }

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public String toString() {
        return "SampleIdentified{"
                + "amount=" + amount
                + '}';
    }
}

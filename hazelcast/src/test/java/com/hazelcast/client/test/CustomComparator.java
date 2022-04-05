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

package com.hazelcast.client.test;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.IterationType;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

/**
 * Custom serializable {@link Comparator} to test non java clients.
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class CustomComparator implements Comparator<Map.Entry>, IdentifiedDataSerializable {

    static final int CLASS_ID = 2;
    private int type;
    private IterationType iterationType;

    public CustomComparator() {
    }

    public CustomComparator(int type, IterationType iterationType) {
        this.type = type;
        this.iterationType = iterationType;
    }

    @Override
    public int compare(Map.Entry e1, Map.Entry e2) {
        String str1;
        String str2;
        switch (iterationType) {
            case KEY:
                str1 = e1.getKey().toString();
                str2 = e2.getKey().toString();
                break;
            case VALUE:
                str1 = e1.getValue().toString();
                str2 = e2.getValue().toString();
                break;
            case ENTRY:
                str1 = e1.getKey().toString() + e1.getValue().toString();
                str2 = e2.getKey().toString() + e2.getValue().toString();
                break;
            default:
                str1 = e1.getKey().toString();
                str2 = e2.getKey().toString();
        }
        switch (type) {
            case 0:
                return str1.compareTo(str2);
            case 1:
                return str2.compareTo(str1);
            case 2:
                return size(str1).compareTo(size(str2));
        }
        return 0;
    }

    @Override
    public int getFactoryId() {
        return IdentifiedFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(iterationType.getId());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = in.readInt();
        iterationType = IterationType.getById((byte) in.readInt());
    }

    private Integer size(String input) {
        return input == null ? 0 : input.length();
    }
}

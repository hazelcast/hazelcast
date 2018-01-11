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

package com.hazelcast.nio.serialization.compatibility;

public class CustomStreamSerializable {
    int i;
    float f;

    public CustomStreamSerializable(int anInt, float aFloat) {
        i = anInt;
        f = aFloat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CustomStreamSerializable that = (CustomStreamSerializable) o;
        if (i != that.i) {
            return false;
        }
        return Float.compare(that.f, f) == 0;
    }

    @Override
    public String toString() {
        return "CustomStreamSerializable";
    }
}

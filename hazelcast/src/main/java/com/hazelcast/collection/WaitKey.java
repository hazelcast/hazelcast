/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.nio.serialization.Data;

/**
 * @ali 1/10/13
 */
public class WaitKey {

    final String name;

    final Data key;

    final String value;

    public WaitKey(String name, Data key, String value) {
        this.name = name;
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WaitKey)) return false;

        WaitKey waitKey = (WaitKey) o;

        if (!key.equals(waitKey.key)) return false;
        if (!name.equals(waitKey.name)) return false;
        if (!value.equals(waitKey.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}

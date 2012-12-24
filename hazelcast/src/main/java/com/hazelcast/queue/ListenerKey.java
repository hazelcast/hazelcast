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

package com.hazelcast.queue;

import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ali 12/24/12
 */
public class ListenerKey{

    private ItemListener listener;
    private String name;

    public ListenerKey(ItemListener listener, String name) {
        this.listener = listener;
        this.name = name;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ListenerKey)) return false;

        ListenerKey that = (ListenerKey) o;

        if (!listener.equals(that.listener)) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    public int hashCode() {
        int result = listener.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}

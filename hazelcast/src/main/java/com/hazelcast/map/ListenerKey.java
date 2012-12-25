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

package com.hazelcast.map;

import com.hazelcast.core.EntryListener;

public class ListenerKey {

    private EntryListener listener;
    private Object key;

    public ListenerKey(EntryListener listener, Object key) {
        this.listener = listener;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ListenerKey that = (ListenerKey) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (!listener.equals(that.listener)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = listener.hashCode();
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }
}

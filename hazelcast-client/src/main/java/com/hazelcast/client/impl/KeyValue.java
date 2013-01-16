/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.IOUtil;

public class KeyValue extends com.hazelcast.impl.base.KeyValue {

    public Object getKey() {
        if (objKey == null) {
            objKey = IOUtil.toObject(key.buffer);
        }
        return objKey;
    }

    public Object getValue() {
        if (objValue == null) {
            if (value != null) {
                objValue = IOUtil.toObject(value.buffer);
            }
        }
        return objValue;
    }

    public Object setValue(Object newValue) {
        Object oldValue = objValue;
        this.objValue = value;
        return oldValue;
    }
}

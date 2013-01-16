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

package com.hazelcast.nio;

public class CustomSerializerAdapter implements TypeSerializer {

    private final CustomSerializer customSerializer;

    public CustomSerializerAdapter(CustomSerializer customSerializer) {
        this.customSerializer = customSerializer;
    }

    public void write(FastByteArrayOutputStream bbos, Object obj) throws Exception {
        customSerializer.write(bbos, obj);
    }

    public Object read(FastByteArrayInputStream bbis) throws Exception {
        return customSerializer.read(bbis);
    }

    public int priority() {
        return 100;
    }

    public boolean isSuitable(Object obj) {
        return true;
    }

    public byte getTypeId() {
        return 1;
    }
}

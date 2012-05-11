/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.nio.Data;

public final class IOUtil {

    public static byte[] toByte(final Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return ((Data) object).buffer;
        }
        return ClientThreadContext.get().toByte(object);
    }

    public static Object toObject(final byte[] bytes) {
        return ClientThreadContext.get().toObject(bytes);
    }

    public static Object toObject(final Data data) {
        return ClientThreadContext.get().toObject(data.buffer);
    }
}

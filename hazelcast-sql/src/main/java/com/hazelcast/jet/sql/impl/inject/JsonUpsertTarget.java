/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

@SuppressWarnings("SpellCheckingInspection")
@NotThreadSafe
class JsonUpsertTarget extends HazelcastJsonUpsertTarget {
    JsonUpsertTarget() {
    }

    @Override
    public Object conclude() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(json);
            oos.close();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
        return baos.toByteArray();
    }
}

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
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

@SuppressWarnings("SpellCheckingInspection")
@NotThreadSafe
class JsonUpsertTarget extends HazelcastJsonUpsertTarget {
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    JsonUpsertTarget() {
    }

    @Override
    public Object conclude() {
        baos.reset();
        try {
            baos.write(json.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
        return baos.toByteArray();
    }
}

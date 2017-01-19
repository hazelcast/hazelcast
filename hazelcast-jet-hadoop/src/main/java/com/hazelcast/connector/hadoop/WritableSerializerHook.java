/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.connector.hadoop;

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class WritableSerializerHook {

    private static final int WRITABLE = -400;

    public static final class WritableSerializer implements SerializerHook<Writable> {

        @Override
        public Class<Writable> getSerializationType() {
            return Writable.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Writable>() {
                @Override
                public int getTypeId() {
                    return WRITABLE;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, Writable writable) throws IOException {
                    out.writeUTF(writable.getClass().getName());
                    writable.write(out);
                }

                @Override
                public Writable read(ObjectDataInput in) throws IOException {
                    String className = in.readUTF();
                    try {
                        Writable instance = ClassLoaderUtil.newInstance(null, className);
                        instance.readFields(in);
                        return instance;
                    } catch (Exception e) {
                        throw rethrow(e);
                    }
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

}

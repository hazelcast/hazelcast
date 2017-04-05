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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public final class StreamUtil {

    private static final String LIST_PREFIX = "__jet_list_";
    private static final int UUID_TRUNCATE_INDEX = 24;

    private StreamUtil() {
    }

    public static String uniqueListName() {
        return LIST_PREFIX + randomName();
    }

    /**
     * @return a unique name for a vertex which includes the given name
     */
    public static String uniqueVertexName(String name) {
        return name + '-' + (UuidUtil.newUnsecureUUID().toString().substring(UUID_TRUNCATE_INDEX));
    }

    public static void executeJob(StreamContext context, DAG dag) {
        JobConfig jobConfig = context.getJobConfig() != null ? context.getJobConfig() : new JobConfig();
        Job job = context.getJetInstance().newJob(dag, jobConfig);
        try {
            job.execute().get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
        context.getStreamListeners().forEach(Runnable::run);
    }

    public static void setPrivateField(Object instance, Class<?> clazz, String name, Object val)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, val);
    }

    private static String randomName() {
        return UuidUtil.newUnsecureUUID().toString();
    }

    /**
     * Checks, that {@code argument} implements {@link Serializable}.
     * It also checks, if the {@code argument} is actually serializable by trying to serialize it.
     * This will reveal early, if all it's fields are serializable.
     *
     * @param argument Object to check
     * @param argumentName Argument name for the exception
     * @throws IllegalArgumentException If {@code argument} is not serializable.
     */
    public static void checkSerializable(Object argument, String argumentName) {
        if (argument != null) {
            if (!(argument instanceof Serializable)) {
                throw new IllegalArgumentException("Argument \"" + argumentName + "\" must be serializable");
            }
            try  (ObjectOutputStream os  = new ObjectOutputStream(new NullOutputStream())) {
                os.writeObject(argument);
            } catch (NotSerializableException | InvalidClassException e) {
                throw new IllegalArgumentException("Argument \"" + argumentName + "\" must be serializable", e);
            } catch (IOException e) {
                // never really thrown, as the underlying stream never throws it
                throw new RuntimeException(e);
            }
        }
    }

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            // do nothing
        }
    }
}

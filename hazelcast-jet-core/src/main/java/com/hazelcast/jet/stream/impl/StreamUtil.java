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
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.util.UuidUtil;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public final class StreamUtil {

    private static final String MAP_PREFIX = "__jet_map_";
    private static final String LIST_PREFIX = "__jet_list_";

    private StreamUtil() {
    }

    public static String uniqueListName() {
        return LIST_PREFIX + randomName();
    }

    public static String uniqueMapName() {
        return MAP_PREFIX + randomName();
    }

    /**
     * @return the name for a writer vertex
     */
    public static String writerVertexName(String name) {
        return name + "-writer";
    }

    /**
     * @return a unique name for a vertex which includes the given name
     */
    public static String uniqueVertexName(String name) {
        return name + "-" + UuidUtil.newUnsecureUUID().toString();
    }

    public static void executeJob(StreamContext context, DAG dag) {
        Job job = context.getJetInstance().newJob(dag);
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
}

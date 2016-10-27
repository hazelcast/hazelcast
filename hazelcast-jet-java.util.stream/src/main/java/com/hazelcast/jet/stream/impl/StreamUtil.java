/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Job;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.util.UuidUtil;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public final class StreamUtil {

    public static final String MAP_PREFIX = "__hz_map_";
    public static final String LIST_PREFIX = "__hz_list_";
    public static final String ENGINE_NAME = "java.util.stream";

    private StreamUtil() {
    }

    public static String randomName() {
        return UuidUtil.newUnsecureUUID().toString().replaceAll("-", "");
    }

    public static String randomName(String prefix) {
        return prefix + randomName();
    }

    public static void executeJob(StreamContext context, DAG dag) {
        Set<Class> classes = context.getClasses();
        JetEngineConfig config = new JetEngineConfig();
        config.addClass(classes.toArray(new Class[classes.size()]));

        JetEngine jetEngine = JetEngine.get(context.getHazelcastInstance(), ENGINE_NAME);
        Job job = jetEngine.newJob(dag);
        job.execute();
        context.getStreamListeners().forEach(Runnable::run);
    }

    public static void setPrivateField(Object instance, Class<?> clazz, String name, Object val)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, val);
    }

    public static HazelcastInstance getHazelcastInstance(DistributedObject object) {
        if (object instanceof AbstractDistributedObject) {
            return ((AbstractDistributedObject) object).getNodeEngine().getHazelcastInstance();
        } else if (object instanceof ClientProxy) {
            try {
                Method method = ClientProxy.class.getDeclaredMethod("getContext");
                method.setAccessible(true);
                ClientContext context = (ClientContext) method.invoke(object);
                return context.getHazelcastInstance();
            } catch (Exception e) {
                throw unchecked(e);
            }
        }
        throw new IllegalArgumentException(object + " is not of a known proxy type");
    }
}

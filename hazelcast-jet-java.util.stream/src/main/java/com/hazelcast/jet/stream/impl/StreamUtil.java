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
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.util.UuidUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static com.hazelcast.jet.impl.util.JetUtil.uncheckedGet;

public final class StreamUtil {

    public static final int DEFAULT_TASK_COUNT = Runtime.getRuntime().availableProcessors();
    public static final String MAP_PREFIX = "__hz_map_";
    public static final String LIST_PREFIX = "__hz_list_";

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
        JobConfig config = new JobConfig();
        config.addClass(classes.toArray(new Class[classes.size()]));
        Job job = JetEngine.getJob(context.getHazelcastInstance(), randomName(), dag, config);
        try {
            uncheckedGet(job.execute());
            context.getStreamListeners().forEach(Runnable::run);
        } finally {
            job.destroy();
        }
    }

    @SuppressWarnings("unchecked")
    public static <E_OUT> Distributed.Function<Pair, E_OUT> defaultFromPairMapper() {
        return pair -> (E_OUT) pair.getValue();
    }

    public static <E_OUT> Distributed.Function<Pair, E_OUT>
    getPairMapper(Pipeline<E_OUT> upstream, Distributed.Function<Pair, E_OUT> mapper) {
        if (upstream instanceof SourcePipeline) {
            SourcePipeline<E_OUT> source = (SourcePipeline<E_OUT>) upstream;
            return source.fromPairMapper();
        }
        return mapper;
    }

    public static EdgeBuilder edgeBuilder(Vertex from, Vertex to) {
        return new EdgeBuilder(randomName(), from, to);
    }

    public static VertexBuilder vertexBuilder(Class<? extends Processor> clazz) {
        return new VertexBuilder(clazz);
    }

    public static void setPrivateField(Object instance, Class<?> clazz, String name, Object val)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, val);
    }

    public static class VertexBuilder {

        private final Class<? extends Processor> clazz;
        private final List<Object> args = new ArrayList<>();
        private Integer taskCount;
        private String name;
        private DAG dag;

        public VertexBuilder(Class<? extends Processor> clazz) {
            this.clazz = clazz;
        }

        public VertexBuilder name(String name) {
            this.name = name + "-" + randomName();
            return this;
        }

        public VertexBuilder args(Object... args) {
            Collections.addAll(this.args, args);
            return this;
        }

        public VertexBuilder taskCount(int taskCount) {
            this.taskCount = taskCount;
            return this;
        }

        public VertexBuilder addToDAG(DAG dag) {
            this.dag = dag;
            return this;
        }

        public Vertex build() {
            Vertex vertex = new Vertex(name == null ? randomName() : name, clazz, args.toArray())
                    .parallelism(taskCount == null ? Runtime.getRuntime().availableProcessors() : taskCount);
            if (dag != null) {
                dag.addVertex(vertex);
            }
            return vertex;
        }
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

    public static class EdgeBuilder extends Edge {

        public EdgeBuilder(String name, Vertex from, Vertex to) {
            super(name, from, to);
        }

        public EdgeBuilder addToDAG(DAG dag) {
            dag.addEdge(this);
            return this;
        }

        public Edge build() {
            return this;
        }
    }
}

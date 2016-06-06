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

import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.processor.ContainerProcessorFactory;
import com.hazelcast.jet.dag.EdgeImpl;
import com.hazelcast.jet.dag.VertexImpl;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.processor.ProcessorDescriptor;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class StreamUtil {

    public static final int DEFAULT_TASK_COUNT = Runtime.getRuntime().availableProcessors();
    public static final String MAP_PREFIX = "__hz_map_";
    public static final String LIST_PREFIX = "__hz_list_";

    private StreamUtil() {
    }

    public static Object result(Future future) {
        try {
            return future.get();
        } catch (ExecutionException | InterruptedException e) {
            throw reThrow(e);
        }
    }

    public static RuntimeException reThrow(Throwable e) {
        if (e instanceof ExecutionException) {
            if (e.getCause() != null) {
                throw reThrow(e.getCause());
            } else {
                throw new RuntimeException(e);
            }
        }

        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }

        return new RuntimeException(e);
    }

    public static String randomName() {
        return UuidUtil.newUnsecureUUID().toString().replaceAll("-", "");
    }

    public static String randomName(String prefix) {
        return prefix + randomName();
    }

    public static void executeApplication(StreamContext context, DAG dag) {
        Application jetApplication = JetEngine.getJetApplication(context.getHazelcastInstance(), randomName());
        try {
            Set<Class> classes = context.getClasses();
            jetApplication.submit(dag, classes.toArray(new Class[classes.size()]));
        } catch (IOException e) {
            throw reThrow(e);
        }
        try {
            result(jetApplication.execute());
            context.getStreamListeners().forEach(Runnable::run);
        } finally {
            result(jetApplication.finalizeApplication());
        }
    }

    public static <E_OUT> Distributed.Function<Tuple, E_OUT> defaultFromTupleMapper() {
        return tuple -> (E_OUT) tuple.getValue(0);
    }

    public static <E_OUT> Distributed.Function<Tuple, E_OUT>
    getTupleMapper(Pipeline<E_OUT> upstream, Distributed.Function<Tuple, E_OUT> mapper) {
        if (upstream instanceof SourcePipeline) {
            SourcePipeline<E_OUT> source = (SourcePipeline<E_OUT>) upstream;
            return source.fromTupleMapper();
        }
        return mapper;
    }

    public static EdgeBuilder edgeBuilder(Vertex from, Vertex to) {
        return new EdgeBuilder(randomName(), from, to);
    }

    public static VertexBuilder vertexBuilder(Class<? extends ContainerProcessorFactory> clazz) {
        return new VertexBuilder(clazz);
    }

    public static void setPrivateField(Object instance, Class<?> clazz, String name, Object val)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, val);
    }

    public static class VertexBuilder {

        private final Class<? extends ContainerProcessorFactory> clazz;
        private final List<Object> args = new ArrayList<>();
        private Integer taskCount;
        private String name;
        private DAG dag;

        public VertexBuilder(Class<? extends ContainerProcessorFactory> clazz) {
            this.clazz = clazz;
        }

        public VertexBuilder name(String name) {
            this.name = name + "-" + randomName();
            return this;
        }

        public VertexBuilder args(Object... args) {
            for (Object arg : args) {
                this.args.add(arg);
            }
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
            VertexImpl vertex = new VertexImpl(name == null ? randomName() : name,
                    ProcessorDescriptor.builder(clazz, args.toArray())
                            .withTaskCount(taskCount == null ? Runtime.getRuntime().availableProcessors() : taskCount)
                            .build());
            if (dag != null) {
                dag.addVertex(vertex);
            }
            return vertex;
        }
    }

    public static class EdgeBuilder extends EdgeImpl.EdgeBuilder {

        private DAG dag;

        public EdgeBuilder(String name, Vertex from, Vertex to) {
            super(name, from, to);
        }

        public EdgeBuilder addToDAG(DAG dag) {
            this.dag = dag;
            return this;
        }

        @Override
        public Edge build() {
            Edge edge = super.build();
            if (dag != null) {
                dag.addEdge(edge);
            }
            return edge;
        }
    }
}

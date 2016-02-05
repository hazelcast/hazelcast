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

package com.hazelcast.jet.spi.processor;

import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkFalse;

import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.impl.processor.descriptor.DefaultProcessorDescriptor;

/**
 * Descriptor with vertex properties;
 * Used inside vertex construction;
 */
public abstract class ProcessorDescriptor implements Serializable {
    private int taskCount = 1;

    public static ProcessorDescriptor create(Class<? extends ContainerProcessorFactory> clazz, Object... args) {
        return new DefaultProcessorDescriptor(clazz, args);
    }

    /**
     * @return - task count in the corresponding vertex-container;
     */
    public int getTaskCount() {
        return this.taskCount;
    }

    /**
     * @return - arguments which will be passed to construct ContainerProcessor by the corresponding factory;
     */
    public abstract Object[] getFactoryArgs();

    /**
     * @return - class of the corresponding factory to construct ContainerProcessor;
     */
    public abstract String getContainerProcessorFactoryClazz();

    /**
     * Creates builder to construct instance of ProcessorDescriptor;
     *
     * @param clazz - class of the corresponding ContainerProcessorFactory;
     * @return - corresponding builder;
     */
    public static Builder builder(Class<? extends ContainerProcessorFactory> clazz) {
        return new Builder(clazz);
    }

    /**
     * Creates builder to construct instance of ProcessorDescriptor;
     *
     * @param clazz - class of the corresponding ContainerProcessorFactory;
     * @return - corresponding builder;
     */
    public static Builder builder(Class<? extends ContainerProcessorFactory> clazz, Object... args) {
        return new Builder(clazz, args);
    }

    /**
     * Builder class to construct ProcessorDescriptor instances;
     */
    public static class Builder {
        private static final String MESSAGE = "ProcessorDescriptor has  already been built";

        private final ProcessorDescriptor processorDescriptor;
        private boolean build;

        public Builder(Class<? extends ContainerProcessorFactory> clazz, Object... args) {
            processorDescriptor = ProcessorDescriptor.create(clazz, args);
        }

        /**
         * Define amount of tasks inside the corresponding container;
         *
         * @param taskCount - amount of task;
         * @return - builder itself;
         */
        public Builder withTaskCount(int taskCount) {
            checkFalse(build, MESSAGE);
            processorDescriptor.taskCount = taskCount;
            return this;
        }

        /**
         * Construct and return ProcessorDescriptor object;
         *
         * @return - corresponding ProcessorDescriptor;
         */
        public ProcessorDescriptor build() {
            checkFalse(build, MESSAGE);
            build = true;
            return processorDescriptor;
        }
    }
}

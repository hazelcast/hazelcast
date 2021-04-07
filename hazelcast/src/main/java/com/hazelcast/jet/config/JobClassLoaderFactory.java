/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * An interface that can be implemented to provide custom class loader for Jet
 * job.
 * <p>
 * The classloader must be serializable: it is set in the {@link
 * JobConfig#setClassLoaderFactory config} and sent to members in a serialized
 * form.
 * <p>
 * It is useful in custom class-loading environments, for example in OSGi.
 *
 * @since 3.0
 */
public interface JobClassLoaderFactory extends Serializable {

    /**
     * Return the class loader instance.
     */
    @Nonnull
    ClassLoader getJobClassLoader();

}

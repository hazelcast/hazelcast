/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import java.io.Serializable;

/**
 * A data sink in a Jet pipeline. It accepts the data the pipeline
 * processed and exports it to an external system.
 *
 * @see Sinks sink factory methods
 *
 * @param <T> the type of the data the sink will receive
 *
 * @since Jet 3.0
 */
public interface Sink<T> extends Serializable {

    /**
     * Returns a descriptive name for this sink.
     */
    String name();

}

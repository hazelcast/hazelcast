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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.connector.HazelcastWriters.ApplyFnEntryProcessor;

/**
 * Constants used for Hazelcast's {@link com.hazelcast.nio.serialization.IdentifiedDataSerializable}
 * mechanism.
 */
public final class SerializationConstants {
    /** Name of the system property that specifies Jet's data serialization factory ID. */
    public static final String JET_DS_FACTORY = "hazelcast.serialization.ds.jet";
    /** Default ID of Jet's data serialization factory. */
    public static final int JET_DS_FACTORY_ID = -10001;
    /** Resolved ID of Jet's data serialization factory. */
    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_DS_FACTORY, JET_DS_FACTORY_ID);
    /** Serialization ID of the {@link com.hazelcast.jet.core.DAG} class. */
    public static final int DAG = 0;
    /** Serialization ID of the {@link Vertex} class. */
    public static final int VERTEX = 1;
    /** Serialization ID of the {@link Edge} class. */
    public static final int EDGE = 2;
    /** Serialization ID of the {@link ApplyFnEntryProcessor} class. */
    public static final int APPLY_FN_ENTRY_PROCESSOR = 3;

    private SerializationConstants() {

    }
}

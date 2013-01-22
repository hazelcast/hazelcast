/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 8/24/12
 */
public final class DataSerializerQueueHook implements DataSerializerHook {

    static final int OFFER = 300;
    static final int POLL = 301;
    static final int PEEK = 302;

    static final int OFFER_BACKUP = 330;
    static final int POLL_BACKUP = 331;

    public Map<Integer, DataSerializableFactory> getFactories() {
        final Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();
        factories.put(OFFER, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new OfferOperation();
            }
        });
        factories.put(OFFER_BACKUP, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new OfferBackupOperation();
            }
        });
        factories.put(POLL, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new PollOperation();
            }
        });
        factories.put(POLL_BACKUP, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new PollBackupOperation();
            }
        });
        factories.put(PEEK, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new PeekOperation();
            }
        });
        return factories;
    }
}

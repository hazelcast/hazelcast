/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.processor.BackupAwareEntryProcessor;
import com.hazelcast.collection.processor.Entry;
import com.hazelcast.config.MultiMapConfig;

import java.util.Collection;

/**
 * @ali 1/3/13
 */
public class RemoveEntryProcessor extends GetEntryProcessor implements BackupAwareEntryProcessor {

    public RemoveEntryProcessor() {
    }

    public RemoveEntryProcessor(boolean binary, MultiMapConfig.ValueCollectionType collectionType) {
        super(binary, collectionType);
    }

    public MultiMapCollectionResponse execute(Entry entry) {
        Collection collection = entry.getValue();
        entry.removeEntry();
        return new MultiMapCollectionResponse(collection, collectionType, isBinary(), entry.getSerializationContext());
    }

    public void executeBackup(Entry entry) {
        entry.removeEntry();
    }
}

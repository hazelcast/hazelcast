/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.internal.util.phonehome.MetricsCollectionContext;
import com.hazelcast.internal.util.phonehome.MetricsProvider;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.impl.VectorPlatformUtil;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_COUNT;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_HEAP_USAGE;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_INDEX_COUNT;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_INDEX_DIMENSIONS;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_TOTAL_BACKUP_COUNTS;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_VECTOR_API_AVAILABLE;

public class VectorCollectionMetricsProvider implements MetricsProvider {

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        VectorCollectionService service = node.getNodeEngine().getService(VectorCollectionService.SERVICE_NAME);

        Config config = node.getConfig();
        int vcCount = config.getVectorCollectionConfigs().size();
        MutableInteger vcIndexCount = new MutableInteger();
        List<String> vcIndexDimensions = new ArrayList<>();
        List<String> vcTotalBackupCounts = new ArrayList<>();

        config.getVectorCollectionConfigs().forEach((name, vcc) -> {
            vcIndexCount.addAndGet(vcc.getVectorIndexConfigs().size());
            vcc.getVectorIndexConfigs().forEach(vic -> vcIndexDimensions.add(Integer.toString(vic.getDimension())));
            vcTotalBackupCounts.add(Integer.toString(vcc.getTotalBackupCount()));
        });

        context.collect(VECTOR_COLLECTION_COUNT, vcCount);
        context.collect(VECTOR_COLLECTION_INDEX_COUNT, vcIndexCount.value);
        context.collect(VECTOR_COLLECTION_INDEX_DIMENSIONS, String.join(",", vcIndexDimensions));
        context.collect(VECTOR_COLLECTION_TOTAL_BACKUP_COUNTS, String.join(",", vcTotalBackupCounts));
        context.collect(VECTOR_COLLECTION_HEAP_USAGE, service.heapBytesUsed());
        context.collect(VECTOR_COLLECTION_VECTOR_API_AVAILABLE, VectorPlatformUtil.isVectorModulePresent());
    }
}

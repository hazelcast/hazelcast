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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.impl.Hints;
import com.hazelcast.vector.impl.query.SingleStageSearcher;
import com.hazelcast.vector.impl.query.TwoStageSearcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VectorCollectionServiceImplTest {
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    NodeEngine nodeEngine;

    @InjectMocks
    VectorCollectionServiceImpl service;

    String collectionName = "vc";

    @Test
    void shouldUseSingleStageSearchOnOneMemberCluster() {
        givenClusterSize(1);

        assertThat(service.getSearcher(collectionName, SearchOptions.builder().build()).getBaseSearcher())
                .isInstanceOf(SingleStageSearcher.class);
    }

    @Test
    void shouldUseTwoStageSearchOnBiggerCluster() {
        givenClusterSize(2);

        assertThat(service.getSearcher(collectionName, SearchOptions.builder().build()).getBaseSearcher())
                .isInstanceOf(TwoStageSearcher.class);
    }

    @Test
    void shouldRespectSingleStageHint() {
        givenClusterSize(2);

        assertThat(service.getSearcher(collectionName, SearchOptions.builder()
                .hint(Hints.FORCE_SINGLE_STAGE_SEARCH, true)
                .build()).getBaseSearcher())
                .isInstanceOf(SingleStageSearcher.class);
    }

    @Test
    void shouldUseDefaultForSingleStageFalseHint() {
        givenClusterSize(2);

        assertThat(service.getSearcher(collectionName, SearchOptions.builder()
                .hint(Hints.FORCE_SINGLE_STAGE_SEARCH, false)
                .build()).getBaseSearcher())
                .isInstanceOf(TwoStageSearcher.class);
    }

    private void givenClusterSize(int value) {
        when(nodeEngine.getClusterService().getSize(any())).thenReturn(value);
    }
}

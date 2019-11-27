/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config.processors;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.internal.config.DomConfigHelper;

import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;

class MergePolicyConfigProcessor implements Processor<MergePolicyConfig> {
    private final Node node;
    private final boolean domLevel3;

    MergePolicyConfigProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
    }

    @Override
    public MergePolicyConfig process() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
        mergePolicyConfig.setPolicy(DomConfigHelper.getTextContent(node, domLevel3).trim());
        final String att = DomConfigHelper.getAttribute(node, "batch-size", domLevel3);
        if (att != null) {
            mergePolicyConfig.setBatchSize(getIntegerValue("batch-size", att));
        }
        return mergePolicyConfig;
    }
}

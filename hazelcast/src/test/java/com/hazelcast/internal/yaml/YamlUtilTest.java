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

package com.hazelcast.internal.yaml;

import org.junit.Test;

public class YamlUtilTest {

    @Test
    public void asMappingReturnsIfMappingPassed() {
        YamlNode genericNode = new YamlMappingImpl(null, "mapping");
        YamlUtil.asMapping(genericNode);
    }

    @Test(expected = YamlException.class)
    public void asMappingThrowsIfNonMappingPassed() {
        YamlNode genericNode = new YamlSequenceImpl(null, "sequence");
        YamlUtil.asMapping(genericNode);
    }

    @Test
    public void asSequenceReturnsIfSequencePassed() {
        YamlNode genericNode = new YamlSequenceImpl(null, "sequence");
        YamlUtil.asSequence(genericNode);
    }

    @Test(expected = YamlException.class)
    public void asSequenceThrowsIfNonSequencePassed() {
        YamlNode genericNode = new YamlMappingImpl(null, "mapping");
        YamlUtil.asSequence(genericNode);
    }

    @Test
    public void asScalarReturnsIfScalarPassed() {
        YamlNode genericNode = new YamlScalarImpl(null, "scalar", "value");
        YamlUtil.asScalar(genericNode);
    }

    @Test(expected = YamlException.class)
    public void asScalarThrowsIfNonScalarPassed() {
        YamlNode genericNode = new YamlMappingImpl(null, "mapping");
        YamlUtil.asScalar(genericNode);
    }
}

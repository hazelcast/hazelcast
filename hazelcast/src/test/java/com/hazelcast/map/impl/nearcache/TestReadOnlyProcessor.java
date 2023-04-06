/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryProcessor;
import javax.annotation.Nullable;
import java.util.Map;

public class TestReadOnlyProcessor implements EntryProcessor<Integer, Integer, Boolean>, ReadOnly {

    @Override
    public Boolean process(Map.Entry<Integer, Integer> entry) {
        return true;
    }


    @Override
    @Nullable
    public EntryProcessor<Integer, Integer, Boolean> getBackupProcessor() {
        return null;
    }
}

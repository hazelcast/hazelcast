/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.jet.core.JetTestSupport;
import org.junit.Before;

public class HdfsTestSupport extends JetTestSupport {

    @Before
    public void before() {
        // Tests fail on windows. If you want to run them, comment out this line and
        // follow this instructions: https://stackoverflow.com/a/35652866/952135
        assumeNotWindows();
    }

}

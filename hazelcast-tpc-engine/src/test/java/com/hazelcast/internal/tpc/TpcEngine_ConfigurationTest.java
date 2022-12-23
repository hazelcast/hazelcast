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

package com.hazelcast.internal.tpc;

import org.junit.Test;


public class TpcEngine_ConfigurationTest {

    @Test(expected = NullPointerException.class)
    public void setEventLoopConfiguration_whenNull() {
        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setEventloopConfiguration(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setEventLoopConfiguration_whenZero() {
        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setEventloopCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setEventLoopConfiguration_whenNegative() {
        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setEventloopCount(-1);
    }
}

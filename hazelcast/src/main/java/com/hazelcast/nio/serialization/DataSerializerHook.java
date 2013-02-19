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

package com.hazelcast.nio.serialization;

import java.util.Map;

/**
 * <p> SPI              [0, 100)
 * <p> MAP              [100, 300)
 * <p> QUEUE            [300, 400)
 * <p> COLLECTION       [400, 500)
 * <p> EXECUTOR         [500, 600)
 * <p> THIRD-PARTIES    [1000, ∞)
 *
 * @mdogan 10/2/12
 */

public interface DataSerializerHook {

    Map<Integer, DataSerializableFactory> getFactories();

}


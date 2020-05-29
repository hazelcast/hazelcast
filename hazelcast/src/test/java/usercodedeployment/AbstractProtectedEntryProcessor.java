/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package usercodedeployment;

import com.hazelcast.map.EntryProcessor;

/**
 * This class allows to test the user code deployment behavior in situations where a parent class of some deployed class is
 * package private.
 */
abstract class AbstractProtectedEntryProcessor
        implements EntryProcessor<Integer, Integer, Integer>, ProtectedInterface, PublicInterface {

    private static final long serialVersionUID = 1L;
}

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

public interface Processor<I, O> {

    /**
     * Process the next item. If item is not processed immediately, the method will be called with the same item again.
     * @param input name of the input for the input
     * @param value value to be processed
     * @param collector collector for the output items
     * @return true if item is processed, false otherwise
     */
    boolean process(String input, I value, OutputCollector<O> collector);

    /**
     * Called after all the input has been exhausted. If false is returned, the method will be called again.
     *
     * @return true if done, false otherwise.
     */
    boolean complete(OutputCollector<O> collector);
}

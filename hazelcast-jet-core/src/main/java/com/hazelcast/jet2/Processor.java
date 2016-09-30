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

/**
 * Does the computation needed to transform one or more named input data streams into one
 * output stream.
 * <p>
 * The processing methods should limit the amount of processing time and data they output per
 * one invocation. A method should return <code>false</code> to signal it's not done with the
 * current item. When the caller is ready to invoke the method again, it will invoke it with
 * the same arguments as the previous time.
 *
 * @param <I> type of input objects
 * @param <O> type of output objects
 */
public interface Processor<I, O> {

    /**
     * Processes the supplied input item and pushes the results into the supplied output collector.
     *
     * @param input name of the input
     * @param item value to be processed
     * @param collector collector of the output items
     * @return <code>true</code> if the method is done with the current item, <code>false</code> otherwise.
     */
    boolean process(String input, I item, OutputCollector<O> collector);

    /**
     * Called after all the input has been exhausted. See {@link #process(String, Object, OutputCollector)}
     * for an explanation on the expected behavior of thi
     *
     * @return <code>true</code> if the method is done completing the processing, <code>false</code> otherwise.
     */
    boolean complete(OutputCollector<? super O> collector);
}

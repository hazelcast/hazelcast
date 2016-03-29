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

package com.hazelcast.jet.api.container.task;


import com.hazelcast.jet.api.data.io.ProducerInputStream;

/**
 * Processor to execute inside task
 * It performs tasks' work:
 *
 * <pre>
 *     1) Reading from producers
 *     2) Writing to consumers
 *     3) Invoke user-level processors
 * </pre>
 */
public interface TaskProcessor {
    /**
     * Execute main processor's operation;
     *
     * @return true - if last processing execution was success, false otherwise;
     * @throws Exception if any exception
     */
    boolean process() throws Exception;

    /**
     * Invoked on reading of next chunk;
     *
     * @param inputStream - next chunk of data;
     * @return true - if last processing execution was success, false otherwise;
     * @throws Exception if any exception
     */
    boolean onChunk(ProducerInputStream<Object> inputStream) throws Exception;

    /**
     * Indicates of some data were produced after last invocation
     *
     * @return true - of data were produced, false - otherwise;
     */
    boolean produced();

    /**
     * Indicates of some data were consumed after last invocation
     *
     * @return true - of data were consumed, false - otherwise;
     */
    boolean consumed();

    /**
     * Indicates if task has been finalized
     *
     * @return - true if task has been finalized; false - otherwise;
     */
    boolean isFinalized();

    /**
     * Resets producer to the initial state;
     */
    void reset();

    /**
     * Will be invoked on initial state to open resources;
     */
    void onOpen();

    /**
     * Will be invoked on initial state to close resources;
     */
    void onClose();

    /**
     * Start finalization process;
     */
    void startFinalization();

    /**
     * Will be invoked when last producer send close signal;
     */
    void onProducersWriteFinished();

    /***
     * @return true if all producers send close signal and no more data in producers
     */
    boolean producersReadFinished();

    /**
     * Will be invoked when last shuffling-network receiver send close signal;
     */
    void onReceiversClosed();
}

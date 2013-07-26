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

package com.hazelcast.client;

import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * @author mdogan 5/6/13
 */
public abstract class CallableClientRequest extends ClientRequest implements Callable {

    final void process() throws Exception {
        final Object result;
        try {
            result = call();
            clientEngine.sendResponse(getEndpoint(), result);
        } catch (Exception e) {
            clientEngine.getLogger(getClass()).warning(e);
            clientEngine.sendResponse(getEndpoint(), e);
        }
    }
}

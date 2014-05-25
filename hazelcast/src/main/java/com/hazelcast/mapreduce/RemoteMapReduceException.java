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

package com.hazelcast.mapreduce;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Field;
import java.util.List;

public class RemoteMapReduceException
        extends HazelcastException {

    private static final Field STACK_TRACE;

    static {
        try {
            STACK_TRACE = Throwable.class.getDeclaredField("stackTrace");
            STACK_TRACE.setAccessible(true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public RemoteMapReduceException(String message, List<Exception> remoteCauses) {
        super(message);
        setStackTraceElements(remoteCauses);
    }

    private void setStackTraceElements(List<Exception> remoteCauses) {
        StackTraceElement[] originalElements = super.getStackTrace();
        int stackTraceSize = originalElements.length;
        for (Exception remoteCause : remoteCauses) {
            stackTraceSize += remoteCause.getStackTrace().length + 1;
        }

        StackTraceElement[] elements = new StackTraceElement[stackTraceSize];
        System.arraycopy(originalElements, 0, elements, 0, originalElements.length);

        int pos = originalElements.length;
        for (Exception remoteCause : remoteCauses) {
            StackTraceElement[] remoteStackTraceElements = remoteCause.getStackTrace();
            elements[pos++] = new StackTraceElement("--- Remote Exception: " + remoteCause.getMessage() + " ---", "", null, 0);
            for (int i = 0; i < remoteStackTraceElements.length; i++) {
                StackTraceElement element = remoteStackTraceElements[i];
                String className = "    " + element.getClassName();
                String methodName = element.getMethodName();
                String fileName = element.getFileName();
                elements[pos++] = new StackTraceElement(className, methodName, fileName, element.getLineNumber());
            }
        }
        setStackTrace0(elements);
    }

    private void setStackTrace0(StackTraceElement[] stackTraceElements) {
        try {
            STACK_TRACE.set(this, stackTraceElements);
        } catch (Exception e) {
            Logger.getLogger(RemoteMapReduceException.class).finest(e);
        }
    }
}

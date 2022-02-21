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

package com.hazelcast.client.impl.protocol.exception;

import java.util.List;

public class ErrorHolder {

    private int errorCode;
    private String className;
    private String message;
    private List<StackTraceElement> stackTraceElements;

    public ErrorHolder(int errorCode, String className, String message, List<StackTraceElement> stackTraceElements) {
        this.errorCode = errorCode;
        this.className = className;
        this.message = message;
        this.stackTraceElements = stackTraceElements;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getClassName() {
        return className;
    }

    public String getMessage() {
        return message;
    }

    public List<StackTraceElement> getStackTraceElements() {
        return stackTraceElements;
    }
}

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

package com.hazelcast.logging;

public class SystemArgsLog extends SystemLog {
    final String msg;
    final Object arg1;
    final Object arg2;
    final Object arg3;
    final int argCount;

    public SystemArgsLog(String msg, Object arg1) {
        this(msg, arg1, null, null, 1);
    }

    public SystemArgsLog(String msg, Object arg1, Object arg2) {
        this(msg, arg1, arg2, null, 2);
    }

    public SystemArgsLog(String msg, Object arg1, Object arg2, Object arg3) {
        this(msg, arg1, arg2, arg3, 3);
    }

    private SystemArgsLog(String msg, Object arg1, Object arg2, Object arg3, int argCount) {
        this.msg = msg;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
        this.argCount = argCount;
    }

    public String getMsg() {
        return msg;
    }

    public Object getArg1() {
        return arg1;
    }

    public Object getArg2() {
        return arg2;
    }

    public Object getArg3() {
        return arg3;
    }

    public int getArgCount() {
        return argCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(msg);
        if (argCount > 0) sb.append(" {").append(arg1).append("}");
        if (argCount > 1) sb.append(" {").append(arg2).append("}");
        if (argCount > 2) sb.append(" {").append(arg3).append("}");
        return sb.toString();
    }
}

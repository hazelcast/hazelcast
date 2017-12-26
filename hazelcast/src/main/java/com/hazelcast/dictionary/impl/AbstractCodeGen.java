/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl;

import static java.lang.String.format;

public class AbstractCodeGen {

    private final StringBuffer codeBuffer = new StringBuffer();
    private int indents;
    private String tab = "    ";
    private String indentString = "";


    public void add(String s, Object... args) {
        codeBuffer.append(format(s, (Object[]) args));
    }

    public void indent() {
        indents++;

        indentString = "";
        for (int k = 0; k < indents; k++) {
            indentString += tab;
        }
    }

    public void unindent() {
        indents--;

        indentString = "";
        for (int k = 0; k < indents; k++) {
            indentString += tab;
        }
    }

    public void addLn() {
        codeBuffer.append("\n");
    }

    public void addLn(String s, Object... args) {
        add(indentString + s + "\n", args);
    }

    public void add(Object arg) {
        codeBuffer.append(arg);
    }

    public String toCode() {
        return codeBuffer.toString();
    }
}

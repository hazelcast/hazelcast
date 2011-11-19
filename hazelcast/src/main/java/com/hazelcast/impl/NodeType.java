/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

public enum NodeType {
    MEMBER(1),
    LITE_MEMBER(2),
    JAVA_CLIENT(3),
    CSHARP_CLIENT(4);

    private int value;

    private NodeType(int type) {
        this.value = type;
    }

    public int getValue() {
        return value;
    }

    public static NodeType create(int value) {
        switch (value) {
            case 1:
                return MEMBER;
            case 2:
                return LITE_MEMBER;
            case 3:
                return JAVA_CLIENT;
            case 4:
                return CSHARP_CLIENT;
            default:
                return null;
        }
    }
}

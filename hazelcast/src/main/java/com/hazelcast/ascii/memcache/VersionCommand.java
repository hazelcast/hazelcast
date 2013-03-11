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

package com.hazelcast.ascii.memcache;

import com.hazelcast.ascii.AbstractTextCommand;

import java.nio.ByteBuffer;

/**
 * User: sancar
 * Date: 3/7/13
 * Time: 10:27 AM
 */
public class VersionCommand extends AbstractTextCommand {

    public static final byte[] version = "VERSION Hazelcast\r\n".getBytes();

    protected VersionCommand(TextCommandType type) {
        super(type);
    }

    public boolean writeTo(ByteBuffer destination) {
        destination.put(version);
        return true;
    }

    public boolean readFrom(ByteBuffer source) {
        return true;
    }
}

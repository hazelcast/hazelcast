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

import com.hazelcast.core.Member;

import java.util.EventObject;
import java.util.logging.LogRecord;

public class LogEvent extends EventObject {
    final LogRecord logRecord;
    final Member member;
    final String groupName;

    public LogEvent(LogRecord logRecord, String groupName, Member member) {
        super(member);
        this.logRecord = logRecord;
        this.groupName = groupName;
        this.member = member;
    }

    public Member getMember() {
        return member;
    }

    public LogRecord getLogRecord() {
        return logRecord;
    }
}

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

package com.hazelcast.security.permission;

/**
 * @ali 10/3/13
 */
public final class ActionConstants {

    public static final String ACTION_ALL = "all";
    public static final String ACTION_CREATE = "create";
    public static final String ACTION_DESTROY = "destroy";
    public static final String ACTION_PUT = "put";
    public static final String ACTION_ADD = "add";
    public static final String ACTION_GET = "get";
    public static final String ACTION_SET = "set";
    public static final String ACTION_REMOVE = "remove";
    public static final String ACTION_OFFER = "offer";
    public static final String ACTION_POLL = "poll";
    public static final String ACTION_TAKE = "take";
    public static final String ACTION_LOCK = "lock";
    public static final String ACTION_LISTEN = "listen";
    public static final String ACTION_PUBLISH = "publish";
    public static final String ACTION_INCREMENT = "increment";
    public static final String ACTION_DECREMENT = "decrement";
    public static final String ACTION_EXECUTE = "execute";
    public static final String ACTION_COUNTDOWN = "countdown";
    public static final String ACTION_ACQUIRE = "acquire";
    public static final String ACTION_RELEASE = "release";
    public static final String ACTION_DRAIN = "drain";
    public static final String ACTION_STATISTICS = "statistics";

    public static final String LISTENER_INSTANCE = "instance";
    public static final String LISTENER_MEMBER = "member";
    public static final String LISTENER_MIGRATION = "migration";

}

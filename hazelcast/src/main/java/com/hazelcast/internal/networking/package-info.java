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

/**
 * Contains the socket connection functionality. So effectively it contains all
 * the networking that is shared between client and server.
 *
 * This package should not contain server/client etc. specific behavior. It should
 * remain neutral, so that the networking package remains reusable for all kinds of
 * purposes. E.g. if we want to totally isolate WAN replication, it should be
 * possible without needing to make (many) modifications to this package.
 */
package com.hazelcast.internal.networking;

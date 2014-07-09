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

/**
 * Provides Hazelcast-based session replication.
 * <p/>
 * To use Hazelcast to provide clustered sessions in a webapp, add the following components to your {@code web.xml}:
 * <code>
 * <pre>
 * &lt;filter&gt;
 *   &lt;filter-name&gt;hazelcastWebFilter&lt;/filter-name&gt;
 *   &lt;filter-class&gt;com.hazelcast.web.WebFilter&lt;/filter-class&gt;
 * &lt;/filter&gt;
 * &lt;filter-mapping&gt;
 *   &lt;filter-name&gt;hazelcastWebFilter&lt;/filter-name&gt;
 *   &lt;url-pattern&gt;/*&lt;/url-pattern&gt;
 *   &lt;dispatcher&gt;FORWARD&lt;/dispatcher&gt;
 *   &lt;dispatcher&gt;INCLUDE&lt;/dispatcher&gt;
 *   &lt;dispatcher&gt;REQUEST&lt;/dispatcher&gt;
 * &lt;/filter-mapping&gt;
 *
 * &lt;listener&gt;
 *   &lt;listener-class&gt;com.hazelcast.web.SessionListener&lt;/listener-class&gt;
 * &lt;/listener&gt;
 * </pre>
 * </code>
 * <p/>
 * {@link com.hazelcast.web.WebFilter WebFilter} should be <i>first</i> in the filter chain to ensure session
 * actions performed in other filters in the chain are replicated. Additionally, note that <i>both</i> the
 * {@link com.hazelcast.web.WebFilter WebFilter} and {@link com.hazelcast.web.SessionListener SessionListener}
 * must be registered for clustered sessions to work properly. The {@code SessionListener} informs the
 * {@code WebFilter} of session timeouts so it can update the cluster accordingly.
 * <p/>
 * {@link com.hazelcast.web.WebFilter WebFilter} supports several {@code &lt;init-param/&gt;} values which can
 * be used to control its behavior. For more details, check its documentation.
 */
package com.hazelcast.web;

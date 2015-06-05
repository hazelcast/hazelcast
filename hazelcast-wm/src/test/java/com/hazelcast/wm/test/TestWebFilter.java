/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wm.test;

import com.hazelcast.util.StringUtil;
import com.hazelcast.web.WebFilter;
import com.hazelcast.web.spring.SpringAwareWebFilter;

import javax.servlet.*;
import java.io.IOException;

public class TestWebFilter implements Filter {

    public static final String USE_SPRING_AWARE_FILTER_PROPERTY = "use-spring-aware-filter";

    private WebFilter delegatedWebFilter;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        boolean useSpringAwareFilter =
                !StringUtil.isNullOrEmpty(
                        System.getProperty(USE_SPRING_AWARE_FILTER_PROPERTY));
        if (!useSpringAwareFilter) {
            String useSpringAwareFilterConfig =
                    filterConfig.getInitParameter(USE_SPRING_AWARE_FILTER_PROPERTY);
            if (!StringUtil.isNullOrEmpty(useSpringAwareFilterConfig)) {
                useSpringAwareFilter = Boolean.parseBoolean(useSpringAwareFilterConfig);
            }
        }
        if (useSpringAwareFilter) {
            delegatedWebFilter = new SpringAwareWebFilter();
        } else {
            delegatedWebFilter = new WebFilter();
        }
        delegatedWebFilter.init(filterConfig);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        delegatedWebFilter.doFilter(request, response, chain);
    }

    @Override
    public void destroy() {
        delegatedWebFilter.destroy();
    }
}

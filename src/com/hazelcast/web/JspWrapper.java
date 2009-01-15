/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.web;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.HttpJspPage;

import com.hazelcast.web.WebFilter.RequestWrapper;

public abstract class JspWrapper extends ServletBase implements HttpJspPage {

	protected JspWrapper() {
	}

	@Override
	public final void init(ServletConfig config) throws ServletException {
		super.init(config);
		jspInit();
		_jspInit();
	}

	@Override
	public final void destroy() {
		super.destroy();
		jspDestroy();
		_jspDestroy();
	}

	@Override
	public final void service(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		if (!(request instanceof RequestWrapper)) {
			HttpServletRequest reqHazel = (HttpServletRequest) request
					.getAttribute(WebFilter.HAZELCAST_REQUEST);
			if (reqHazel != null)
				request = reqHazel;
		}
		_jspService(request, response);
	}

	public void jspInit() {
	}

	public void _jspInit() {
	}

	public void jspDestroy() {
	}

	protected void _jspDestroy() {
	}

	public abstract void _jspService(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException;

}
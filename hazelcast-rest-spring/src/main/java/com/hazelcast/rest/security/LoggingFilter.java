/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.rest.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.security.SignatureException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class LoggingFilter implements Filter {
    private static String secret;
    private final String header = "Authorization";
    private final String prefix = "Bearer ";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws
            IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;
        if (checkJWTToken(req)) {
            try {
                Claims claims = validateToken(req);
                System.out.println("claims.getSubject: " + claims.getSubject());
                System.out.println("Logging Request " +  req.getMethod() + " : " +  req.getRequestURI());
                chain.doFilter(request, response);
            } catch (MalformedJwtException | SignatureException e) {
                res.setStatus(HttpServletResponse.SC_FORBIDDEN);
                res.sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage());
            }

        } else {
            res.setStatus(HttpServletResponse.SC_FORBIDDEN);
            res.sendError(HttpServletResponse.SC_FORBIDDEN);
        }
    }

    private boolean checkJWTToken(HttpServletRequest request) {
        String authenticationHeader = request.getHeader(header);
        return authenticationHeader != null && authenticationHeader.startsWith(prefix);
    }

    private Claims validateToken(HttpServletRequest request) {
        String jwtToken = request.getHeader(header).replace(prefix, "");
        return Jwts.parser().setSigningKey(secret.getBytes()).parseClaimsJws(jwtToken).getBody();
    }

    public static void setSecret(String secret) {
        LoggingFilter.secret = secret;
    }
}

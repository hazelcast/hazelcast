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
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.UnsupportedJwtException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.annotation.Nonnull;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;


public class JWTAuthorizationFilter extends OncePerRequestFilter {

    private static String secret;
    private final String header = "Authorization";
    private final String prefix = "Bearer ";


    public static void setSecret(String secret) {
        JWTAuthorizationFilter.secret = secret;
    }

    @Override
    protected void doFilterInternal(@Nonnull HttpServletRequest request,
                                    @Nonnull HttpServletResponse response,
                                    @Nonnull FilterChain chain) throws IOException {
        try {
            if (checkJWTToken(request, response)) {
                Claims claims = validateToken(request);
                if (claims.get("authorities") != null) {
                    List<GrantedAuthority> grantedAuthorities = AuthorityUtils
                            .commaSeparatedStringToAuthorityList(claims.get("authorities").toString());

                    if (grantedAuthorities.contains(new SimpleGrantedAuthority("ADMIN"))) {
                        setUpSpringAuthentication(claims, grantedAuthorities);
                    } else if (isAuthorized(request, grantedAuthorities)) {
                        setUpSpringAuthentication(claims, grantedAuthorities);
                    } else {
                        SecurityContextHolder.clearContext();
                    }
                } else {
                    SecurityContextHolder.clearContext();
                }
            } else {
                SecurityContextHolder.clearContext();
            }

            chain.doFilter(request, response);
        } catch (ExpiredJwtException | UnsupportedJwtException | MalformedJwtException | ServletException e) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            response.sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage());
        }
    }

    private Claims validateToken(HttpServletRequest request) {
        String jwtToken = request.getHeader(header).replace(prefix, "");
        return Jwts.parser().setSigningKey(secret.getBytes()).parseClaimsJws(jwtToken).getBody();
    }

    private void setUpSpringAuthentication(Claims claims, List<GrantedAuthority> grantedAuthorities) {
        String authorities = claims.get("authorities").toString();
        UsernamePasswordAuthenticationToken auth = new UsernamePasswordAuthenticationToken(claims.getSubject(), null,
                grantedAuthorities);
        SecurityContextHolder.getContext().setAuthentication(auth);

    }

    private boolean checkJWTToken(HttpServletRequest request, HttpServletResponse res) {
        String authenticationHeader = request.getHeader(header);
        return authenticationHeader != null && authenticationHeader.startsWith(prefix);
    }

    private boolean isAuthorized(HttpServletRequest request, List<GrantedAuthority> grantedAuthorities) {
        String requestURI = request.getRequestURI();
        System.out.println("requestURI: " + requestURI);
        if (requestURI.contains("/maps/test") && grantedAuthorities.contains(new SimpleGrantedAuthority("USER"))) {
            return true;
        } else if (requestURI.contains("/cluster") && grantedAuthorities.contains(new SimpleGrantedAuthority("CLUSTER"))) {
            return true;
        }
        return false;
    }
}

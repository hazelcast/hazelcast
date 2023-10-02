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
package com.hazelcast.rest.service;

import com.hazelcast.rest.security.JWTAuthorizationFilter;
import com.hazelcast.rest.util.NodeEngineImplHolder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class BearerTokenService {
    @Autowired
    NodeEngineImplHolder nodeEngineImplHolder;

    private final int expirationTimeMillis = 600000;

    public String getJWTToken() {
        List<GrantedAuthority> grantedAuthorities = AuthorityUtils
                .commaSeparatedStringToAuthorityList("ROLE_USER");

        String token = Jwts
                .builder()
                .setId(getSecret())
                .setSubject(getSecret())
                .claim("authorities",
                        grantedAuthorities.stream()
                                .map(GrantedAuthority::getAuthority)
                                .collect(Collectors.toList()))
                .setIssuedAt(new Date(System.currentTimeMillis()))
                .setExpiration(new Date(System.currentTimeMillis() + expirationTimeMillis))
                .signWith(SignatureAlgorithm.HS256,
                        getSecret().getBytes()).compact();

        return "Bearer " + token;
    }

    private String getSecret() {
        String secret = nodeEngineImplHolder.getNodeEngine().getHazelcastInstance().getConfig().getSecurityConfig()
                .getRealmConfig(nodeEngineImplHolder.getNodeEngine().getHazelcastInstance().getConfig()
                        .getSecurityConfig().getMemberRealm())
                .getUsernamePasswordIdentityConfig().toString();
        JWTAuthorizationFilter.setSecret(secret);

        return secret;
    }
}

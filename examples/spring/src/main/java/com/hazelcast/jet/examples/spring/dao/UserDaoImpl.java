/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.spring.dao;

import com.hazelcast.jet.examples.spring.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dao implementation for {@code User}, queries db with jdbc template
 * and maps rows to {@code User} objects
 */
@Repository
public class UserDaoImpl implements UserDao {

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Override
    public User findByName(String name) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("name", name);
        String sql = "SELECT * FROM users WHERE name=:name";

        return namedParameterJdbcTemplate.queryForObject(sql, params, new UserMapper());
    }

    @Override
    public List<User> findAll() {
        String sql = "SELECT * FROM users";

        return namedParameterJdbcTemplate.query(sql, new HashMap<>(), new UserMapper());
    }

    private static final class UserMapper implements RowMapper<User> {

        public User mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new User()
                    .setId(rs.getInt("id"))
                    .setName(rs.getString("name"))
                    .setEmail(rs.getString("email"));
        }
    }

}

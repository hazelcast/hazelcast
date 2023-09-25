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
package com.hazelcast.rest.controller;

import com.hazelcast.spi.impl.NodeEngineImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/maps")
public class MapController {

    private NodeEngineImpl nodeEngine;

    @GetMapping(value = "/{mapName}/{key}")
    @Operation(summary = "Find value by key in a map",
            tags = {"map controller"},
            description = "Returns if there is a corresponding value for given mapName and key",
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK"),
                    @ApiResponse(responseCode = "204", description = "No Content"),
                    @ApiResponse(responseCode = "400", description = "Bad Request"),
                    @ApiResponse(responseCode = "404", description = "Not found"),
                    @ApiResponse(responseCode = "500", description = "Internal Server Error")
            })
    ResponseEntity<String> getMap(@Parameter(in = ParameterIn.PATH, description = "The map name", required = true,
            schema = @Schema()) @PathVariable("mapName") String mapName,
                                  @Parameter(in = ParameterIn.PATH, description = "The key of map", required = true,
                                          schema = @Schema()) @PathVariable("key") String key) {

        Object value = nodeEngine.getHazelcastInstance().getMap(mapName).get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(value.toString());
    }

    @PostMapping(value = "/{mapName}/{key}")
    @Operation(summary = "Add a new value to the map",
            tags = {"map controller"},
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK"),
                    @ApiResponse(responseCode = "204", description = "No Content"),
                    @ApiResponse(responseCode = "400", description = "Bad Request"),
                    @ApiResponse(responseCode = "500", description = "Internal Server Error")
            })
    ResponseEntity<String> postMap(@Parameter(in = ParameterIn.PATH, description = "The map name", required = true,
            schema = @Schema()) @PathVariable("mapName") String mapName,
                                   @Parameter(in = ParameterIn.PATH, description = "The key of map", required = true,
                                           schema = @Schema()) @PathVariable("key") String key,
                                   @Parameter(in = ParameterIn.DEFAULT, description = "", required = true,
                                           schema = @Schema()) @RequestBody String value) {
        nodeEngine.getHazelcastInstance().getMap(mapName).set(key, value);
        return ResponseEntity.ok("(" + key + " : " + value + ") is added to map " + mapName);
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public void setNodeEngine(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }
}

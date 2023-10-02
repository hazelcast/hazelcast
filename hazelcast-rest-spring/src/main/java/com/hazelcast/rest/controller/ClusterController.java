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

import com.hazelcast.rest.model.ClusterStatusModel;
import com.hazelcast.rest.service.ClusterService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class ClusterController {
    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping(value = "/cluster")
    @Operation(summary = "Check the status of the cluster",
            tags = {"Cluster Controller"},
            description = "Check the status of the cluster",
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK", content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = ClusterStatusModel.class)
                    )),
                    @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = Exception.class)
                    )),
                    @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = Exception.class)
                    ))
            })
    ResponseEntity<ClusterStatusModel> getClusterStatus() {
        ClusterStatusModel clusterStatus = clusterService.getClusterStatus();
        return ResponseEntity.ok().body(clusterStatus);
    }
}

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

import com.hazelcast.rest.constant.Paths;
import com.hazelcast.rest.model.MemberDetailModel;
import com.hazelcast.rest.model.StatusCodeAndMessage;
import com.hazelcast.rest.service.MemberService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
public class MemberController {
    private final MemberService memberService;

    public MemberController(MemberService memberService) {
        this.memberService = memberService;
    }

    @GetMapping(value = Paths.V1_MEMBER_BASE_PATH, params = {"page", "size"})
    @Operation(summary = "Member details",
            tags = {"Member Controller"},
            description = "Member details",
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK", content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = MemberDetailModel.class)
                    )),
                    @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = String.class)
                    ))
            })
    ResponseEntity<List<MemberDetailModel>> getMembers(
            @RequestParam(defaultValue = "false", name = "self") boolean self,
            @RequestParam(defaultValue = "0", name = "page") int page,
            @RequestParam(defaultValue = "10", name = "size") int size) {
        if (self) {
            return ResponseEntity.ok().body(List.of(memberService.getCurrentMember()));
        }
        List<MemberDetailModel> members = memberService.getMembers(page, size);
        return ResponseEntity.ok().body(members);
    }

    @GetMapping(value = Paths.V1_MEMBER_UUID_PATH)
    @Operation(summary = "Member detail",
            tags = {"Member Controller"},
            description = "Member detail",
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK", content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = MemberDetailModel.class)
                    )),
                    @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = StatusCodeAndMessage.class)
                    ))
            })
    ResponseEntity<?> getMemberWithUuid(@Parameter(in = ParameterIn.PATH, description = "The member uuid", required = true,
            schema = @Schema()) @PathVariable("member-uuid") UUID uuid) {
        System.out.println("UUID: " + uuid);
        MemberDetailModel member = memberService.getMemberWithUuid(uuid);
        if (member == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new StatusCodeAndMessage(
                    HttpStatus.NOT_FOUND.value(),
                    "Member could not be found with the uuid: " + uuid));
        } else {
            return ResponseEntity.ok().body(member);
        }
    }
}

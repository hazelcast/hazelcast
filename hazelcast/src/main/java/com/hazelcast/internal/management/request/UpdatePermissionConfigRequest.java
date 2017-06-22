package com.hazelcast.internal.management.request;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.PermissionConfigDTO;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by emrah on 12/06/2017.
 */
public class UpdatePermissionConfigRequest implements ConsoleRequest {

    private Set<PermissionConfig> permissionConfigs;

    public UpdatePermissionConfigRequest() {

    }

    public UpdatePermissionConfigRequest(Set<PermissionConfig> permissionConfigs) {
        this.permissionConfigs = permissionConfigs;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        JsonArray permissionConfigsArr = new JsonArray();
        for (PermissionConfig permissionConfig : permissionConfigs) {
            permissionConfigsArr.add(new PermissionConfigDTO(permissionConfig).toJson());
        }
        root.add("permissionConfigs", permissionConfigsArr);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        JsonArray confArray = json.get("permissionConfigs").asArray();
        permissionConfigs = new HashSet<PermissionConfig>(confArray.size());
        for (JsonValue permissionConfigDTO : confArray.values()) {
            PermissionConfigDTO dto = new PermissionConfigDTO();
            dto.fromJson(permissionConfigDTO.asObject());
            permissionConfigs.add(dto.getPermissionConfig());
        }
    }

    public Set<PermissionConfig> getPermissionConfigs() {
        return permissionConfigs;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_UPDATE_PERMISSON_CONFIG;
    }

    @Override
    public Object readResponse(JsonObject in) throws IOException {
        return null;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        mcs.getHazelcastInstance().getSecurityService().refreshClientPermissions(permissionConfigs);
    }
}

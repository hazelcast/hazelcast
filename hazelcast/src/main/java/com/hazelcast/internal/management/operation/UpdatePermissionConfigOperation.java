package com.hazelcast.internal.management.operation;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.internal.management.dto.PermissionConfigDTO;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by emrah on 09/06/2017.
 */
public class UpdatePermissionConfigOperation extends AbstractManagementOperation {

    private Set<PermissionConfig> permissionConfigs;

    public UpdatePermissionConfigOperation() {

    }

    public UpdatePermissionConfigOperation(Set<PermissionConfig> permissionConfigs) {
        this.permissionConfigs = permissionConfigs;
    }

    @Override
    public int getId() {
        return ManagementDataSerializerHook.UPDATE_PERMISSION_CONFIG_OPERATION;
    }

    @Override
    public void run() throws Exception {
        HazelcastInstanceImpl instance = (HazelcastInstanceImpl) getNodeEngine().getHazelcastInstance();
        instance.node.securityContext.refreshClientPermissions(permissionConfigs);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(permissionConfigs.size());
        for (PermissionConfig permissionConfig : permissionConfigs) {
            new PermissionConfigDTO(permissionConfig).writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int configSize = in.readInt();
        permissionConfigs = new HashSet<PermissionConfig>(configSize);
        for (int i = 0; i < configSize; i++) {
            PermissionConfigDTO permissionConfigDTO = new PermissionConfigDTO();
            permissionConfigDTO.readData(in);
            permissionConfigs.add(permissionConfigDTO.getPermissionConfig());
        }
    }
}

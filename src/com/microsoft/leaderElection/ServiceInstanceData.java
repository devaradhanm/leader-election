package com.microsoft.leaderElection;

import lombok.Getter;
import lombok.Setter;
import org.codehaus.jackson.map.annotate.JsonRootName;

@JsonRootName("details")
public class ServiceInstanceData {
    @Getter
    @Setter
    private boolean isLeader = false;

    @Getter
    @Setter
    private String nodeId;

    @Getter
    @Setter
    private int port;

    public ServiceInstanceData() {
        this("", 0, false);
    }

    public ServiceInstanceData(String nodeId, int port,  boolean isLeader) {
        this.isLeader = isLeader;
        this.nodeId = nodeId;
        this.port = port;
    }
}

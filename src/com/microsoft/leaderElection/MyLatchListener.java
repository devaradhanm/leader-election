package com.microsoft.leaderElection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.IOException;

public class MyLatchListener implements LeaderLatchListener {
    private final ServiceInstanceData payload;
    private final JsonInstanceSerializer<ServiceInstanceData> serializer = new JsonInstanceSerializer<>(ServiceInstanceData.class);
    private ServiceInstance serviceInstance;
    private ServiceDiscovery<ServiceInstanceData> discovery;
    private final String applicationName = "test-application";

    public MyLatchListener(String nodeId, int port) {
        payload = new ServiceInstanceData(nodeId, port, false);
    }

    public void init(CuratorFramework client) throws Exception {
        serviceInstance = ServiceInstance.<ServiceInstanceData>builder()
                .uriSpec(new UriSpec("{scheme}://test-app.com:{port}"))
                .address("localhost")
                .id(payload.getNodeId())
                .port(payload.getPort())
                .name(applicationName)
                .payload(payload)
                .build();

        discovery = ServiceDiscoveryBuilder.builder(ServiceInstanceData.class)
                .basePath("service-discovery")
                .client(client)
                .thisInstance(serviceInstance)
                .watchInstances(true)
                .serializer(serializer)
                .build();

        System.out.println("Starting discovery");
        discovery.start();
    }

    public void close() throws IOException {
        System.out.println("Closing latch listener");
        if(discovery != null) {
            discovery.close();
            discovery = null;
        }
    }

    @Override
    public void isLeader() {
        updateDiscovery(true);
    }

    @Override
    public void notLeader() {
        updateDiscovery(false);
    }

    private void updateDiscovery(Boolean isLeader) {
        payload.setLeader(isLeader);

        try {
            System.out.println("Updating leader status to " + isLeader);
            discovery.updateService(serviceInstance);
        } catch (Exception e) {
            System.out.println("Error when updating service discovery" + e);
        }
    }
}

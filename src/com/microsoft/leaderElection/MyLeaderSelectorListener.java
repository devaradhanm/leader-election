package com.microsoft.leaderElection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.nio.channels.FileChannel.*;

public class MyLeaderSelectorListener extends LeaderSelectorListenerAdapter {
    private final ServiceInstanceData payload;
    private final JsonInstanceSerializer<ServiceInstanceData> serializer = new JsonInstanceSerializer<>(ServiceInstanceData.class);
    private ServiceInstance serviceInstance;
    private ServiceDiscovery<ServiceInstanceData> discovery;
    private final String applicationName = "test-application";

    public MyLeaderSelectorListener(String nodeId, int port) {
        payload = new ServiceInstanceData(nodeId, port, false);
    }

    public void init(CuratorFramework client) throws Exception {
        serviceInstance = ServiceInstance.<ServiceInstanceData>builder()
                .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
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
    public void takeLeadership(CuratorFramework client) throws Exception {
        try {
            FileChannel channel = open(Path.of("D:\\temp\\lock.txt"), StandardOpenOption.APPEND);
            channel.lock();

            try {
                byte[] inputBytes = ("Acquired lock by node - " + serviceInstance.getId() + ". \t").getBytes();
                channel.write(ByteBuffer.wrap(inputBytes));

                try {
                    payload.setLeader(true);
                    System.out.println("Acquired leadership. - " + serviceInstance.getId());
                    System.out.flush();

                    discovery.updateService(serviceInstance);
                } catch (Exception e) {
                    System.out.println("Error when updating service discovery" + e);
                } finally {
                    synchronized (this) {
                        this.wait(5000 + RandomNumberGenerator.getRandomNumberUsingInts(1000, 2000));
                    }
                }
            }
            finally {
                var inputBytes = ("Released lock by node - " + serviceInstance.getId() + "\n").getBytes();
                channel.write(ByteBuffer.wrap(inputBytes));
                channel.close();
            }

        } catch (Exception e) {
            System.out.println("Error thrown :" + e);
        }
        finally {
            System.out.println("Released leadership. - " + serviceInstance.getId());
            System.out.flush();
        }

    }
}

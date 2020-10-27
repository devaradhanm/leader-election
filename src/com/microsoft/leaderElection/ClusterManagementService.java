package com.microsoft.leaderElection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;

public class ClusterManagementService {
    private final String zookeeperConnection = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183,127.0.0.1:2184,127.0.0.1:2185,127.0.0.1:2186,127.0.0.1:2187";

    private final String nodePath = "/test-application";
    private final String leaderPath = nodePath+"/leader";

    private LeaderLatch leaderLatch;
    private MyLatchListener latchListener;
    private MyLeaderSelectorListener leaderSelectorListener;
    private LeaderSelector leaderSelector;
    private CuratorFramework client;

    public void init(String nodeId, int port, Boolean useLeaderSelector) throws Exception {
        final int CONNECTION_TIMEOUT_MS = Integer.valueOf(60 * 1000);
        final int SESSION_TIMEOUT_MS = Integer.valueOf(60 * 1000);
        client = CuratorFrameworkFactory.newClient(zookeeperConnection, SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS, new ExponentialBackoffRetry(1000, 3));
        client.start();
        try {
            client.blockUntilConnected();
            if(client.checkExists().creatingParentContainersIfNeeded().forPath(nodePath) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(nodePath);
            }
//            System.out.println("Copying file content to node");
//            var fileBytes = Files.readAllBytes(Paths.get("C:\\Users\\demuruge\\Downloads\\CodeFlowInstaller.exe"));
//            client.setData().forPath(nodePath, fileBytes);

//            System.out.println("Reading node content to file");
//            var content = client.getData().forPath(nodePath);
//            Files.write(Paths.get("C:\\Users\\demuruge\\Downloads\\eclipse-inst-jre-win64-copied.exe"), content);

            latchListener = new MyLatchListener(nodeId, port);
            latchListener.init(client);

            if(useLeaderSelector) {
                System.out.println("Starting leader selector");
                leaderSelectorListener = new MyLeaderSelectorListener(nodeId, port);
                leaderSelectorListener.init(client);

                leaderSelector = new LeaderSelector(client, nodePath, leaderSelectorListener);
                leaderSelector.autoRequeue();
                leaderSelector.start();
                var leader = leaderSelector.getLeader();
                System.out.println("Current leader is " + leader == null? "none" : leader.getId());
            }
            else {
                System.out.println("Starting leader latch");
                leaderLatch = new LeaderLatch(client, leaderPath, nodeId);
                leaderLatch.addListener(latchListener);
                leaderLatch.start();
                var leader = leaderLatch.getLeader();
                System.out.println("Current leader is " + leader == null? "none" : leader.getId());
            }

        } catch (Exception e) {
            System.out.println("Error when starting leadership listener" + e);
            throw e;
        }
    }

    public void close() throws IOException {
        System.out.println("Closing Service startup");
        if(leaderLatch != null) {
            leaderLatch.close();
            leaderLatch = null;
        }
        if(latchListener != null) {
            latchListener.close();
            latchListener = null;
        }

        if(leaderSelector != null) {
            leaderSelector.close();
            leaderSelector = null;
        }
        if(leaderSelectorListener != null) {
            leaderSelectorListener.close();
            leaderSelectorListener = null;
        }

        if(client != null) {
            client.close();
            client = null;
        }
        System.out.println("Closed all connections");
    }
}

package com.microsoft.leaderElection;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws Exception {
        ClusterManagementService startup = new ClusterManagementService();

        int port = RandomNumberGenerator.getRandomNumberUsingInts(5000, 7000);
        System.out.println("Selected port " + port);

        Boolean useLeaderSelector = true;
        InputStreamReader in = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(in);

        startup.init("node-"+ port, port, useLeaderSelector);

        while(true) {
            System.out.println("Interactive options:\n" +
                    "\trelease -> releases leadership listeners. \n" +
                    "\texit -> kills the current process gracefully\n" +
                    "\trejoin -> rejoins leadership pool\n" +
                    "\tuseLatch -> uses Latch listener. This does not support requeue itself. We have to tear down & join again\n");

            String input = br.readLine();

            switch (input) {
                case "release" :
                    System.out.println("releasing...");
                    startup.close();
                    break;
                case "exit" :
                    System.out.println("quiting process...");
                    startup.close();
                    return;
                case "rejoin" :
                    startup.close();
                    System.out.println("rejoining...");
                    startup.init("node-"+ port, port, useLeaderSelector);
                    break;
                case "useLatch":
                    useLeaderSelector = false;
                    startup.close();
                    System.out.println("rejoining...");
                    startup.init("node-"+ port, port, useLeaderSelector);
                    break;
                default:
                    System.out.println("Invalid option. Try again!!!");
            }
        }

    }


}

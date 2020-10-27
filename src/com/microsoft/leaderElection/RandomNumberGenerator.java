package com.microsoft.leaderElection;

import java.util.Random;

public class RandomNumberGenerator {
    public static int getRandomNumberUsingInts(int min, int max) {
        Random random = new Random();
        return random.ints(min, max)
                .findFirst()
                .getAsInt();
    }
}

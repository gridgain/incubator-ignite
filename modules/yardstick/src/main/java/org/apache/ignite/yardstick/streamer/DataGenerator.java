package org.apache.ignite.yardstick.streamer;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** */
class DataGenerator {
    /** */
    private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    /** */
    private static final int ALPHABET_LENGTH = ALPHABET.length();

    /** */
    static CustomKey randomKey() {
        return new CustomKey(
            randomString(20),
            randomString(20),
            randomString(20)
        );
    }

    /** */
    static CustomValue randomValue() {
        int numOfObjects = randomInt(5);

        List<CustomNestedValue> list = new ArrayList<>(numOfObjects);
        for (int i = 0; i < numOfObjects; i++) {
            list.add(new CustomNestedValue(
                LocalDate.now(),
                new BigDecimal(randomLong()),
                randomString(20),
                randomLong()
            ));
        }

        return new CustomValue(
            randomString(256),
            list,
            LocalDateTime.now()
        );
    }

    /** */
    private static String randomString(int len) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(ALPHABET.charAt(rnd.nextInt(ALPHABET_LENGTH)));
        return sb.toString();
    }

    /** */
    private static int randomInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    /** */
    private static int randomInt(int bound) {
        return ThreadLocalRandom.current().nextInt(bound);
    }

    /** */
    private static long randomLong() {
        return ThreadLocalRandom.current().nextLong();
    }
}

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
            randomString(12),
            randomString(12),
            randomString(12),
            randomInt()
        );
    }

    /** */
    static CustomValue randomValue() {
        int numOfTxn = randomInt(5);

        List<CustomNestedValue> txns = new ArrayList<>(numOfTxn);
        for (int i = 0; i < numOfTxn; i++) {
            txns.add(new CustomNestedValue(
                LocalDate.now(),
                new BigDecimal(randomLong()),
                randomString(10),
                randomLong(),
                randomLong(),
                randomInt(),
                randomString(10)
            ));
        }

        return new CustomValue(
            randomString(12),
            randomString(12),
            randomString(12),
            randomString(4),
            randomString(10),
            new BigDecimal(randomLong()),
            randomInt(),
            randomString(10),
            randomString(10),
            randomString(10),
            txns,
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

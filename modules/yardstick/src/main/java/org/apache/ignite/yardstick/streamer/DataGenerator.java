/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    static CustomValue randomValue(int payloadSize) {
        int numOfObjects = 5;

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
            randomString(payloadSize),
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

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

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.cache.CacheArguments;
import org.apache.ignite.internal.commandline.cache.CacheCommands;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.Commands.CACHE;
import static org.apache.ignite.internal.commandline.Commands.WAL;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests Command Handler parsing arguments.
 */
public class CommandHandlerParsingTest {
    /**
     * validate_indexes command arguments parsing and validation
     */
    @Test
    public void testValidateIndexArguments() {
        //happy case for all parameters
        try {
            int expectedCheckFirst = 10;
            int expectedCheckThrough = 11;
            UUID nodeId = UUID.randomUUID();

            Command command = parseArgs(Arrays.asList(
                CACHE.text(),
                VALIDATE_INDEXES.text(),
                "cache1, cache2",
                nodeId.toString(),
                CHECK_FIRST.toString(),
                Integer.toString(expectedCheckFirst),
                CHECK_THROUGH.toString(),
                Integer.toString(expectedCheckThrough)
            ));

            assertTrue(command instanceof CacheCommands);

            CacheArguments args = ((CacheCommands)command).arg();

            assertEquals("nodeId parameter unexpected value", nodeId, args.nodeId());
            assertEquals("checkFirst parameter unexpected value", expectedCheckFirst, args.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedCheckThrough, args.checkThrough());
        }
        catch (IllegalArgumentException e) {
            fail("Unexpected exception: " + e);
        }

        try {
            int expectedParam = 11;
            UUID nodeId = UUID.randomUUID();

            Command command = parseArgs(Arrays.asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    nodeId.toString(),
                    CHECK_THROUGH.toString(),
                    Integer.toString(expectedParam)
                ));

            assertTrue(command instanceof CacheCommands);

            CacheArguments args = ((CacheCommands)command).arg();

            assertNull("caches weren't specified, null value expected", args.caches());
            assertEquals("nodeId parameter unexpected value", nodeId, args.nodeId());
            assertEquals("checkFirst parameter unexpected value", -1, args.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedParam, args.checkThrough());
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            parseArgs(
                Arrays.asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    CHECK_FIRST.toString(),
                    "0"
                )
            );

            fail("Expected exception hasn't been thrown");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            parseArgs(Arrays.asList(CACHE.text(), VALIDATE_INDEXES.text(), CHECK_THROUGH.toString()));

            fail("Expected exception hasn't been thrown");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    @Test
    public void testFindAndDeleteGarbage() {
        String nodeId = UUID.randomUUID().toString();
        String delete = FindAndDeleteGarbageArg.DELETE.toString();
        String groups = "group1,grpoup2,group3";

        List<List<String>> lists = generateArgumentList(
            FIND_AND_DELETE_GARBAGE.text(),
            new T2<>(nodeId, false),
            new T2<>(delete, false),
            new T2<>(groups, false)
        );

        for (List<String> list : lists) {
            Command command = parseArgs(list);


            assertTrue(command instanceof CacheCommands);

            CacheArguments args = ((CacheCommands)command).arg();

            if (list.contains(nodeId)) {
                assertEquals("nodeId parameter unexpected value", nodeId, args.nodeId().toString());
            }
            else {
                assertNull(args.nodeId());
            }

            assertEquals(list.contains(delete), args.delete());

            if (list.contains(groups)) {
                assertEquals(3, args.groups().size());
            }
            else {
                assertNull(args.groups());
            }
        }
    }

    private List<List<String>> generateArgumentList(String subcommand, T2<String, Boolean>...optional) {
        List<List<T2<String, Boolean>>> lists = generateAllCombinations(Arrays.asList(optional), (x) -> x.get2());

        ArrayList<List<String>> res = new ArrayList<>();

        ArrayList<String> empty = new ArrayList<>();

        empty.add(CACHE.text());
        empty.add(subcommand);

        res.add(empty);

        for (List<T2<String, Boolean>> list : lists) {
            ArrayList<String> arg = new ArrayList<>(empty);

            list.forEach(x -> arg.add(x.get1()));

            res.add(arg);
        }

        return res;
    }

    private <T> List<List<T>> generateAllCombinations(List<T> source, Predicate<T> stopFunc) {
        List<List<T>> res = new ArrayList<>();

        for (int i = 0; i < source.size(); i++) {
            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            generateAllCombinations(Collections.singletonList(removed), sourceCopy, stopFunc, res);
        }

        return res;
    }


    private <T> void generateAllCombinations(List<T> res, List<T> source, Predicate<T> stopFunc, List<List<T>> acc) {
        acc.add(res);

        if (stopFunc != null && stopFunc.test(res.get(res.size() - 1))) {
            return;
        }

        if (source.size() == 1) {
            ArrayList<T> list = new ArrayList<>(res);

            list.add(source.get(0));

            acc.add(list);

            return;
        }

        for (int i = 0; i < source.size(); i++) {
            ArrayList<T> res0 = new ArrayList<>(res);

            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            res0.add(removed);

            generateAllCombinations(res0, sourceCopy, stopFunc, acc);
        }
    }

    /**
     * Test that experimental command (i.e. WAL command) is disabled by default.
     */
    @Test
    public void testExperimentalCommandIsDisabled() {
        System.clearProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);

        try {
            parseArgs(Arrays.asList(WAL.text(), WAL_PRINT));
        }
        catch (Throwable e) {
            e.printStackTrace();

            assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            parseArgs(Arrays.asList(WAL.text(), WAL_DELETE));
        }
        catch (Throwable e) {
            e.printStackTrace();

            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    /**
     * Tests parsing and validation for the SSL arguments.
     */
    @Test
    public void testParseAndValidateSSLArguments() {
        for (Commands cmd : Commands.values()) {
            if (cmd == Commands.CACHE || cmd == Commands.WAL)
                continue; // --cache subcommand requires its own specific arguments.

            try {
                parseArgs(asList("--truststore"));

                fail("expected exception: Expected truststore");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            Command command = parseArgs(asList("--keystore", "testKeystore", "--keystore-password", "testKeystorePassword", "--keystore-type", "testKeystoreType",
                "--truststore", "testTruststore", "--truststore-password", "testTruststorePassword", "--truststore-type", "testTruststoreType",
                "--ssl-key-algorithm", "testSSLKeyAlgorithm", "--ssl-protocol", "testSSLProtocol", cmd.text()));

            ConnectionAndSslParameters args = command.commonArguments();

            assertEquals("testSSLProtocol", args.sslProtocol());
            assertEquals("testSSLKeyAlgorithm", args.sslKeyAlgorithm());
            assertEquals("testKeystore", args.sslKeyStorePath());
            assertArrayEquals("testKeystorePassword".toCharArray(), args.sslKeyStorePassword());
            assertEquals("testKeystoreType", args.sslKeyStoreType());
            assertEquals("testTruststore", args.sslTrustStorePath());
            assertArrayEquals("testTruststorePassword".toCharArray(), args.sslTrustStorePassword());
            assertEquals("testTruststoreType", args.sslTrustStoreType());

            assertEquals(cmd.command(), command);
        }
    }


    /**
     * Tests parsing and validation for user and password arguments.
     */
    @Test
    public void testParseAndValidateUserAndPassword() {
        for (Commands cmd : Commands.values()) {
            if (cmd == Commands.CACHE || cmd == Commands.WAL)
                continue; // --cache subcommand requires its own specific arguments.

            try {
                parseArgs(asList("--user"));

                fail("expected exception: Expected user name");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                parseArgs(asList("--password"));

                fail("expected exception: Expected password");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            Command command = parseArgs(asList("--user", "testUser", "--password", "testPass", cmd.text()));
            ConnectionAndSslParameters args = command.commonArguments();

            assertEquals("testUser", args.getUserName());
            assertEquals("testPass", args.getPassword());
            assertEquals(cmd.command(), command);
        }
    }

    /**
     * Tests parsing and validation  of WAL commands.
     */
    @Test
    public void testParseAndValidateWalActions() {
        Command command = parseArgs(Arrays.asList(WAL.text(), WAL_PRINT));

        assertEquals(WAL.command(), command);

        T2<String, String> arg = ((WalCommands)command).arg();

        assertEquals(WAL_PRINT, arg.get1());

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        command = parseArgs(Arrays.asList(WAL.text(), WAL_DELETE, nodes));

        arg = ((WalCommands)command).arg();

        assertEquals(WAL_DELETE, arg.get1());

        assertEquals(nodes, arg.get2());

        try {
            parseArgs(Collections.singletonList(WAL.text()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            parseArgs(Arrays.asList(WAL.text(), UUID.randomUUID().toString()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests that the auto confirmation flag was correctly parsed.
     */
    @Test
    public void testParseAutoConfirmationFlag() {
        for (Commands cmd : Commands.values()) {
            if (cmd != Commands.DEACTIVATE
                && cmd != Commands.BASELINE
                && cmd != Commands.TX)
                continue;

            Command command = parseArgs(asList(cmd.text()));
            ConnectionAndSslParameters args = command.commonArguments();

            assertEquals(cmd.command(), command);
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());
            assertFalse(args.autoConfirmation());

            switch (cmd) {
                case DEACTIVATE: {
                    command = parseArgs(asList(cmd.text(), "--yes"));

                    args = command.commonArguments();

                    assertEquals(cmd.command(), command);
                    assertEquals(DFLT_HOST, args.host());
                    assertEquals(DFLT_PORT, args.port());
                    assertTrue(args.autoConfirmation());

                    break;
                }
                case BASELINE: {
                    for (String baselineAct : asList("add", "remove", "set")) {
                        command = parseArgs(asList(cmd.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        args = command.commonArguments();

                        assertEquals(cmd.command(), command);
                        assertEquals(DFLT_HOST, args.host());
                        assertEquals(DFLT_PORT, args.port());
                        assertTrue(args.autoConfirmation());

                        BaselineArguments arg = ((BaselineCommand)command).arg();
                        assertEquals(baselineAct, arg.getCmd().text());
                        assertEquals(new HashSet<>(Arrays.asList("c_id1","c_id2")), new HashSet<>(arg.getConsistentIds()));
                    }

                    break;
                }

                case TX: {
                    command = parseArgs(asList(cmd.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));
                    args = command.commonArguments();

                    assertEquals(cmd.command(), command);
                    assertEquals(DFLT_HOST, args.host());
                    assertEquals(DFLT_PORT, args.port());
                    assertTrue(args.autoConfirmation());

                    VisorTxTaskArg txTaskArg = ((TxCommands)command).arg();

                    assertEquals("xid1", txTaskArg.getXid());
                    assertEquals(10_000, txTaskArg.getMinDuration().longValue());
                    assertEquals(VisorTxOperation.KILL, txTaskArg.getOperation());
                }
            }
        }
    }

    /**
     * Tests host and port arguments.
     * Tests connection settings arguments.
     */
    @Test
    public void testConnectionSettings() {
        for (Commands cmd : Commands.values()) {
            if (cmd == Commands.CACHE || cmd == Commands.WAL)
                continue; // --cache subcommand requires its own specific arguments.

            Command command = parseArgs(asList(cmd.text()));
            ConnectionAndSslParameters args = command.commonArguments();

            assertEquals(cmd.command(), command);
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            command = parseArgs(asList("--port", "12345", "--host", "test-host", "--ping-interval", "5000",
                "--ping-timeout", "40000", cmd.text()));
            args = command.commonArguments();

            assertEquals(cmd.command(), command);
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());

            try {
                parseArgs(asList("--port", "wrong-port", cmd.text()));

                fail("expected exception: Invalid value for port:");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                parseArgs(asList("--ping-interval", "-10", cmd.text()));

                fail("expected exception: Ping interval must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                parseArgs(asList("--ping-timeout", "-20", cmd.text()));

                fail("expected exception: Ping timeout must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * test parsing dump transaction arguments
     */
    @Test
    public void testTransactionArguments() {
        ConnectionAndSslParameters args;

        parseArgs(asList("--tx"));

        try {
            parseArgs(asList("--tx", "minDuration"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "minDuration", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "minSize"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "minSize", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "label"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "label", "tx123["));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "servers", "nodes", "1,2,3"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        Command command = parseArgs(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE",
            "--servers"));

        args = command.commonArguments();

        VisorTxTaskArg arg = ((TxCommands)command).arg();

        assertEquals(Long.valueOf(120 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(10), arg.getMinSize());
        assertEquals(Integer.valueOf(100), arg.getLimit());
        assertEquals(VisorTxSortOrder.SIZE, arg.getSortOrder());
        assertEquals(VisorTxProjection.SERVER, arg.getProjection());

        command = parseArgs(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));
        args = command.commonArguments();
        arg = ((TxCommands)command).arg();

        assertEquals(Long.valueOf(130 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(1), arg.getMinSize());
        assertEquals(Integer.valueOf(60), arg.getLimit());
        assertEquals(VisorTxSortOrder.DURATION, arg.getSortOrder());
        assertEquals(VisorTxProjection.CLIENT, arg.getProjection());

        command = parseArgs(asList("--tx", "--nodes", "1,2,3"));

        args = command.commonArguments();
        arg = ((TxCommands)command).arg();

        assertNull(arg.getProjection());
        assertEquals(Arrays.asList("1", "2", "3"), arg.getConsistentIds());
    }


    private Command parseArgs(List<String> args) {
        return new CommandArgParser(new CommandLogger()).
            parseAndValidate(args.iterator());
    }
}

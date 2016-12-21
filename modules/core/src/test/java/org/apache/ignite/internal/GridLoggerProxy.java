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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_GRID_NAME;

/**
 *
 */
public class GridLoggerProxy implements IgniteLogger, LifecycleAware, Externalizable {

    /**
     * If this system property is present the Ignite will test all strings passed into log.
     * It's a special mode for testing purposes. All debug information will be generated,
     * but only suspicious will be written into log with "error" level and special prefix SENSITIVE>
     */
    public static final String IGNITE_LOG_TEST_SENSITIVE = "IGNITE_LOG_TEST_SENSITIVE";

    /** */
    private static final long serialVersionUID = 0L;
    /** Whether or not to log grid name. */
    private static final boolean logGridName = System.getProperty(IGNITE_LOG_GRID_NAME) != null;
    /** Test sensitive mode. */
    private static final boolean TEST_SENSITIVE = System.getProperty(IGNITE_LOG_TEST_SENSITIVE) != null;
    /** Prefix for all suspicious sensitive data */
    private static final String SENSITIVE_PREFIX = "SENSITIVE> ";
    /** Sensitive patterns: excluding */
    private static final Pattern[] EXCLUDE_PATTERNS;
    /** Sensitive patterns: including */
    private static final Pattern[] INCLUDE_PATTERNS;
    /** Excluding logger categories */
    private static final Pattern EXCLUDE_CATEGORY_P = Pattern.compile("Test($|\\$)|\\.tests?\\.");
    /** */
    private static ThreadLocal<IgniteBiTuple<String, Object>> stash = new ThreadLocal<IgniteBiTuple<String, Object>>() {
        @Override protected IgniteBiTuple<String, Object> initialValue() {
            return new IgniteBiTuple<>();
        }
    };
    /** */
    private static ThreadLocal<StringBuilder> sbLoc = new ThreadLocal<StringBuilder>() {
        @Override protected StringBuilder initialValue() {
            return new StringBuilder(SENSITIVE_PREFIX);
        }
    };

    static {
        EXCLUDE_PATTERNS = readFromResource("_Exclude.txt");
        INCLUDE_PATTERNS = readFromResource("_Include.txt");
    }

    /** */
    @GridToStringInclude
    private IgniteLogger impl;
    /** */
    private String gridName;
    /** */
    private String id8;
    /** */
    @GridToStringInclude
    private Object ctgr;
    /** Whether testing sensitive is enabled for the logger */
    private boolean testSensitive;

    /**
     * No-arg constructor is required by externalization.
     */
    public GridLoggerProxy() {
        // No-op.
    }

    /**
     * @param impl Logger implementation to proxy to.
     * @param ctgr Optional logger category.
     * @param gridName Grid name (can be {@code null} for default grid).
     * @param id8 Node ID.
     */
    @SuppressWarnings({"IfMayBeConditional", "SimplifiableIfStatement"})
    public GridLoggerProxy(IgniteLogger impl, @Nullable Object ctgr, @Nullable String gridName, String id8) {
        assert impl != null;

        this.impl = impl;
        this.ctgr = ctgr;
        this.gridName = gridName;
        this.id8 = id8;
        this.testSensitive = TEST_SENSITIVE && (ctgr == null || !EXCLUDE_CATEGORY_P.matcher(ctgr.toString()).find());
        if (TEST_SENSITIVE && ctgr == null && gridName == null && id8 == null)
            impl.warning("Test sensitive mode is enabled");
    }

    /** Read sensitive patterns from resource */
    private static Pattern[] readFromResource(String suffix) {
        ArrayList<Pattern> lst = new ArrayList<>();
        final Class<GridLoggerProxy> cls = GridLoggerProxy.class;
        String resPath = cls.getSimpleName() + suffix;
        try (InputStream inStr = cls.getResourceAsStream(resPath);
             BufferedReader rdr = new BufferedReader(new InputStreamReader(inStr))) {
            String ln;
            while ((ln = rdr.readLine()) != null) {
                ln = ln.trim();
                if (ln.isEmpty())
                    continue;
                lst.add(Pattern.compile(ln));
            }
        }
        catch (Exception ex) {
            System.err.println("Error loading sensitive patterns from resource " + resPath + ": " + ex);
            ex.printStackTrace();
        }
        return lst.toArray(new Pattern[lst.size()]);
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (impl instanceof LifecycleAware)
            ((LifecycleAware)impl).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        U.stopLifecycleAware(this, Collections.singleton(impl));
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        assert ctgr != null;

        return new GridLoggerProxy(impl.getLogger(ctgr), ctgr, gridName, id8);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return impl.fileName();
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (testSensitive) {
            testSensitive(msg);
            if (impl.isTraceEnabled())
                impl.trace(enrich(msg));
        }
        else
            impl.trace(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (testSensitive) {
            testSensitive(msg);
            if (impl.isDebugEnabled())
                impl.debug(enrich(msg));
        }
        else
            impl.debug(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (testSensitive) {
            testSensitive(msg);
            if (impl.isInfoEnabled())
                impl.info(enrich(msg));
        }
        else
            impl.info(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        if (testSensitive)
            testSensitive(msg);
        impl.warning(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, Throwable e) {
        if (testSensitive)
            testSensitive(msg, e);
        impl.warning(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        if (testSensitive)
            testSensitive(msg);
        impl.error(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, Throwable e) {
        if (testSensitive)
            testSensitive(msg, e);
        impl.error(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return testSensitive || impl.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return testSensitive || impl.isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return testSensitive || impl.isInfoEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return !testSensitive && impl.isQuiet();
    }

    /**
     * Enriches the log message with grid name if {@link IgniteSystemProperties#IGNITE_LOG_GRID_NAME}
     * system property is set.
     *
     * @param m Message to enrich.
     * @return Enriched message or the original one.
     */
    private String enrich(@Nullable String m) {
        return logGridName && m != null ? "<" + gridName + '-' + id8 + "> " + m : m;
    }

    /**
     * Testing the message by sensitive patterns.<br>
     * All sensitive data will be logged with {@link #SENSITIVE_PREFIX}
     */
    private void testSensitive(String m) {
        String f = findSensitive(m);
        if (f != null)
            logSensitive(f, m);
    }

    /**
     * Testing the message and exceptions chain by sensitive patterns.<br>
     * All sensitive data will be logged with {@link #SENSITIVE_PREFIX}
     */
    private void testSensitive(String m, Throwable e) {
        String f = findSensitive(m);
        if (f != null)
            logSensitive(f, m, e);
        while (e != null) {
            f = findSensitive(e.getMessage());
            if (f != null)
                logSensitive(f, m, e);
            e = e.getCause();
        }
    }

    /**
     * Check the message by sensitive patterns
     *
     * @return the matching string was found or {@code null} if not match
     */
    private String findSensitive(String msg) {
        if (msg == null || msg.isEmpty())
            return null;
        for (Pattern p : EXCLUDE_PATTERNS) {
            Matcher m = p.matcher(msg);
            if (m.find())
                return null;
        }
        for (Pattern p : INCLUDE_PATTERNS) {
            Matcher m = p.matcher(msg);
            if (m.find())
                return m.group();
        }
        return null;
    }

    /** Logging the message with {@link #SENSITIVE_PREFIX} */
    private void logSensitive(String f, String m) {
        logSensitive(f, m, null);
    }

    /** Logging the message and the exception with {@link #SENSITIVE_PREFIX} */
    private void logSensitive(String f, String m, Throwable e) {
        StringBuilder sb = sbLoc.get();
        sb.setLength(SENSITIVE_PREFIX.length());
        sb.append("Found: ").append(f).
            append(", category = ").append(ctgr).
            append(", message:\n").append(m);
        if (e != null)
            sb.append("exception:\n").append(e.getClass()).append(": ").append(e.getMessage());
        impl.error(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, gridName);
        out.writeObject(ctgr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<String, Object> t = stash.get();

        t.set1(U.readString(in));
        t.set2(in.readObject());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<String, Object> t = stash.get();

            Object ctgrR = t.get2();

            IgniteLogger log = IgnitionEx.localIgnite().log();

            return ctgrR != null ? log.getLogger(ctgrR) : log;
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridLoggerProxy.class, this);
    }
}

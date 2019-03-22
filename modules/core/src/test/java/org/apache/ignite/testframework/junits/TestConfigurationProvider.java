package org.apache.ignite.testframework.junits;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridTestPortUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class TestConfigurationProvider {
    /** Ip finder for TCP discovery. */
    public static final TcpDiscoveryIpFinder LOCAL_IP_FINDER = new TcpDiscoveryVmIpFinder(false) {{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    private final TestIgniteInstanceNameProvider instanceNameProvider;

    private final long timeout;

    private final boolean localIpFinderIsDefault;

    public TestConfigurationProvider(
        TestIgniteInstanceNameProvider provider,
        long timeout,
        boolean localIpFinderIsDefault
    ) {
        instanceNameProvider = provider;
        this.timeout = timeout;
        this.localIpFinderIsDefault = localIpFinderIsDefault;
    }

    /**
     * Optimizes configuration to achieve better test performance.
     *
     * @param cfg Configuration.
     * @return Optimized configuration (by modifying passed in one).
     */
    protected IgniteConfiguration optimize(IgniteConfiguration cfg) {
        if (cfg.getLocalHost() == null) {
            if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi) {
                cfg.setLocalHost("127.0.0.1");

                if (((TcpDiscoverySpi)cfg.getDiscoverySpi()).getJoinTimeout() == 0)
                    ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(10000);
            }
            else {
                try {
                    cfg.setLocalHost(U.getLocalHost().getHostAddress());
                }
                catch (IOException ignore) { /** Just ignore. */ }
            }
        }

        // Do not add redundant data if it is not needed.
        if (cfg.getIncludeProperties() == null)
            cfg.setIncludeProperties();

        return cfg;
    }


    /**
     * Loads configuration from the given Spring XML file.
     *
     * @param springCfgPath Path to file.
     * @return Grid configuration.
     * @throws IgniteCheckedException If load failed.
     */
    @SuppressWarnings("deprecation")
    protected IgniteConfiguration loadConfiguration(String springCfgPath) throws IgniteCheckedException {
        URL cfgLocation = U.resolveIgniteUrl(springCfgPath);

        if (cfgLocation == null)
            cfgLocation = U.resolveIgniteUrl(springCfgPath, false);

        assert cfgLocation != null;

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(cfgLocation.toString());
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate Spring XML application context.", e);
        }

        Map cfgMap;

        try {
            // Note: Spring is not generics-friendly.
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null)
            throw new IgniteCheckedException("Failed to find a single grid factory configuration in: " + springCfgPath);

        if (cfgMap.isEmpty())
            throw new IgniteCheckedException("Can't find grid factory configuration in: " + springCfgPath);
        else if (cfgMap.size() > 1)
            throw new IgniteCheckedException("More than one configuration provided for cache load test: " + cfgMap.values());

        IgniteConfiguration cfg = (IgniteConfiguration)cfgMap.values().iterator().next();

        cfg.setNodeId(UUID.randomUUID());

        return cfg;
    }

    /**
     * @return Grid test configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration getConfiguration() throws Exception {
        // Generate unique Ignite instance name.
        return getConfiguration(instanceNameProvider.getTestIgniteInstanceName());
    }

    /**
     * This method should be overridden by subclasses to change configuration parameters.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Grid configuration used for starting of grid.
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) {
        IgniteTestResources rsrcs = getTestResources();
        IgniteConfiguration cfg1 = new IgniteConfiguration();

        cfg1.setIgniteInstanceName(igniteInstanceName);
        cfg1.setGridLogger(rsrcs.getLogger());
        cfg1.setMarshaller(rsrcs.getMarshaller());
        cfg1.setNodeId(rsrcs.getNodeId());
        cfg1.setIgniteHome(rsrcs.getIgniteHome());
        cfg1.setMBeanServer(rsrcs.getMBeanServer());
        cfg1.setPeerClassLoadingEnabled(true);
        cfg1.setMetricsLogFrequency(0);

        cfg1.setConnectorConfiguration(null);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setLocalPort(GridTestPortUtils.getNextCommPort(getClass())); //TODO
        commSpi.setTcpNoDelay(true);

        cfg1.setCommunicationSpi(commSpi);

        TcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        if (GridTestUtils.isDebugMode()) {
            cfg1.setFailureDetectionTimeout(timeout <= 0 ? getDefaultTestTimeout() : timeout);
            cfg1.setNetworkTimeout(Long.MAX_VALUE / 3);
        }
        else {
            // Set network timeout to 10 sec to avoid unexpected p2p class loading errors.
            cfg1.setNetworkTimeout(10_000);

            cfg1.setFailureDetectionTimeout(10_000);
            cfg1.setClientFailureDetectionTimeout(10_000);
        }

        // Set metrics update interval to 1 second to speed up tests.
        cfg1.setMetricsUpdateFrequency(1000);

        if (localIpFinderIsDefault) {
            discoSpi.setIpFinder(LOCAL_IP_FINDER);
        }
        else {
            assert sharedStaticIpFinder != null : "Shared static IP finder should be initialized at this point.";

            discoSpi.setIpFinder(sharedStaticIpFinder);
        }

        cfg1.setDiscoverySpi(discoSpi);

        SharedFsCheckpointSpi cpSpi = new SharedFsCheckpointSpi();

        Collection<String> paths = new ArrayList<>();

        paths.add(getDefaultCheckpointPath(cfg1.getMarshaller()));

        cpSpi.setDirectoryPaths(paths);

        cfg1.setCheckpointSpi(cpSpi);

        cfg1.setEventStorageSpi(new MemoryEventStorageSpi());

        cfg1.setIncludeEventTypes(EventType.EVTS_ALL);

        cfg1.setFailureHandler(getFailureHandler(igniteInstanceName));

        IgniteConfiguration cfg = cfg1;

        cfg.setNodeId(null);

        if (GridTestProperties.getProperty(GridTestProperties.BINARY_COMPACT_FOOTERS) != null) {
            if (!Boolean.valueOf(GridTestProperties.getProperty(GridTestProperties.BINARY_COMPACT_FOOTERS))) {
                BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

                if (bCfg == null) {
                    bCfg = new BinaryConfiguration();

                    cfg.setBinaryConfiguration(bCfg);
                }

                bCfg.setCompactFooter(false);
            }
        }

        if (Boolean.valueOf(GridTestProperties.getProperty(GridTestProperties.BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER))) {
            BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

            if (bCfg == null) {
                bCfg = new BinaryConfiguration();

                cfg.setBinaryConfiguration(bCfg);
            }

            bCfg.setNameMapper(new BinaryBasicNameMapper(true));
        }

        if (igniteInstanceName != null && igniteInstanceName.matches(".*\\d")) {
            String idStr = UUID.randomUUID().toString();

            if (instanceNameProvider.instanceNameWasGeneratedByProvider(igniteInstanceName)) {
                String idxStr = String.valueOf(instanceNameProvider.getTestIgniteInstanceIndex(igniteInstanceName));

                while (idxStr.length() < 5)
                    idxStr = '0' + idxStr;

                char[] chars = idStr.toCharArray();

                for (int i = 0; i < idxStr.length(); i++)
                    chars[chars.length - idxStr.length() + i] = idxStr.charAt(i);

                cfg.setNodeId(UUID.fromString(new String(chars)));
            }
            else {
                char[] chars = idStr.toCharArray();

                chars[0] = igniteInstanceName.charAt(igniteInstanceName.length() - 1);
                chars[1] = '0';

                chars[chars.length - 3] = '0';
                chars[chars.length - 2] = '0';
                chars[chars.length - 1] = igniteInstanceName.charAt(igniteInstanceName.length() - 1);

                cfg.setNodeId(UUID.fromString(new String(chars)));
            }
        }

        if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi)
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(timeout);

        return cfg;
    }

    public IgniteConfiguration readyConfiguration(String igniteInstanceName) {
        return optimize(getConfiguration(igniteInstanceName));
    }


    /**
     * @param marshaller Marshaller to get checkpoint path for.
     * @return Path for specific marshaller.
     */
    @SuppressWarnings({"IfMayBeConditional"})
    protected String getDefaultCheckpointPath(Marshaller marshaller) {
        if (marshaller instanceof JdkMarshaller)
            return SharedFsCheckpointSpi.DFLT_DIR_PATH + "/jdk/";
        else
            return SharedFsCheckpointSpi.DFLT_DIR_PATH + '/' + marshaller.getClass().getSimpleName() + '/';
    }
}

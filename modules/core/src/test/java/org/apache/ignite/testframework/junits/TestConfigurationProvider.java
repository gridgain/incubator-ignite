package org.apache.ignite.testframework.junits;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import static org.apache.ignite.testframework.config.GridTestProperties.BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER;

public class TestConfigurationProvider {

    /**
     * Optimizes configuration to achieve better test performance.
     *
     * @param cfg Configuration.
     * @return Optimized configuration (by modifying passed in one).
     * @throws IgniteCheckedException On error.
     */
    protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        if (cfg.getLocalHost() == null) {
            if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi) {
                cfg.setLocalHost("127.0.0.1");

                if (((TcpDiscoverySpi)cfg.getDiscoverySpi()).getJoinTimeout() == 0)
                    ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(10000);
            }
            else
                cfg.setLocalHost(getTestResources().getLocalHost());
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
        return getConfiguration(getTestIgniteInstanceName());
    }

    /**
     * This method should be overridden by subclasses to change configuration parameters.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Grid configuration used for starting of grid.
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(igniteInstanceName, getTestResources());

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

        if (Boolean.valueOf(GridTestProperties.getProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER))) {
            BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

            if (bCfg == null) {
                bCfg = new BinaryConfiguration();

                cfg.setBinaryConfiguration(bCfg);
            }

            bCfg.setNameMapper(new BinaryBasicNameMapper(true));
        }

        if (igniteInstanceName != null && igniteInstanceName.matches(".*\\d")) {
            String idStr = UUID.randomUUID().toString();

            if (igniteInstanceName.startsWith(getTestIgniteInstanceName())) {
                String idxStr = String.valueOf(getTestIgniteInstanceIndex(igniteInstanceName));

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
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(getTestTimeout());

        return cfg;
    }


    /**
     * This method should be overridden by subclasses to change configuration parameters.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param rsrcs Resources.
     * @throws Exception If failed.
     * @return Grid configuration used for starting of grid.
     */
    @SuppressWarnings("deprecation")
    protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs)
        throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);
        cfg.setGridLogger(rsrcs.getLogger());
        cfg.setMarshaller(rsrcs.getMarshaller());
        cfg.setNodeId(rsrcs.getNodeId());
        cfg.setIgniteHome(rsrcs.getIgniteHome());
        cfg.setMBeanServer(rsrcs.getMBeanServer());
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setMetricsLogFrequency(0);

        cfg.setConnectorConfiguration(null);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        commSpi.setTcpNoDelay(true);

        cfg.setCommunicationSpi(commSpi);

        TcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        if (isDebug()) {
            cfg.setFailureDetectionTimeout(getTestTimeout() <= 0 ? getDefaultTestTimeout() : getTestTimeout());
            cfg.setNetworkTimeout(Long.MAX_VALUE / 3);
        }
        else {
            // Set network timeout to 10 sec to avoid unexpected p2p class loading errors.
            cfg.setNetworkTimeout(10_000);

            cfg.setFailureDetectionTimeout(10_000);
            cfg.setClientFailureDetectionTimeout(10_000);
        }

        // Set metrics update interval to 1 second to speed up tests.
        cfg.setMetricsUpdateFrequency(1000);

        if (!isMultiJvm()) {
            assert sharedStaticIpFinder != null : "Shared static IP finder should be initialized at this point.";

            discoSpi.setIpFinder(sharedStaticIpFinder);
        }
        else
            discoSpi.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        SharedFsCheckpointSpi cpSpi = new SharedFsCheckpointSpi();

        Collection<String> paths = new ArrayList<>();

        paths.add(getDefaultCheckpointPath(cfg.getMarshaller()));

        cpSpi.setDirectoryPaths(paths);

        cfg.setCheckpointSpi(cpSpi);

        cfg.setEventStorageSpi(new MemoryEventStorageSpi());

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        cfg.setFailureHandler(getFailureHandler(igniteInstanceName));

        return cfg;
    }

    public IgniteConfiguration readyConfiguration(String igniteInstanceName) {
        return optimize(getConfiguration(igniteInstanceName));
    }
}

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.servicegrid.SimpleMapServiceImpl;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class CacheNotConfiguredExample {

    TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        new CacheNotConfiguredExample().doMain();
    }

    void doMain() throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(5);
        service.submit(this::startServer);
//        ignite.cluster().active(false);
//        Thread.sleep(5000l);
        service.submit(this::startClient);
//       System.out.println("AAA");
//        ignite.cluster().active(true);

    }

    Ignite startClient() {
        IgniteConfiguration igniteCfg = igniteConfiguration();
        igniteCfg.setIgniteInstanceName("Client Node");
        igniteCfg.setClientMode(true);

        return Ignition.start(igniteCfg);
    }

    Ignite startServer() {
        IgniteConfiguration igniteCfg = igniteConfiguration();
        igniteCfg.setActiveOnStart(true);
        igniteCfg.setIgniteInstanceName("Server Node");
        igniteCfg.setServiceConfiguration(serviceConfiguration());
        igniteCfg.setClientMode(false);

        return Ignition.start(igniteCfg);
    }

    IgniteConfiguration igniteConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setActiveOnStart(true);
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    ServiceConfiguration serviceConfiguration() {
        ServiceConfiguration svcCfg = new ServiceConfiguration();
        svcCfg.setName("service");
        svcCfg.setTotalCount(1);
        svcCfg.setService(new SimpleMapServiceImpl<Integer, String>());
        return svcCfg;
    }
}

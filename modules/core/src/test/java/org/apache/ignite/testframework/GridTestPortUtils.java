package org.apache.ignite.testframework;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;

public class GridTestPortUtils {

    /** */
    private static final Map<Class<?>, String> addrs = new HashMap<>();

    /** */
    private static final Map<Class<?>, Integer> mcastPorts = new HashMap<>();

    /** */
    private static final Map<Class<?>, Integer> discoPorts = new HashMap<>();

    /** */
    private static final Map<Class<?>, Integer> commPorts = new HashMap<>();

    /** */
    private static int[] addr;

    /** */
    private static final int default_mcast_port = 50000;

    /** */
    private static final int max_mcast_port = 54999;

    /** */
    private static final int default_comm_port = 45000;

    /** */
    private static final int max_comm_port = 49999;

    /** */
    private static final int default_disco_port = 55000;

    /** */
    private static final int max_disco_port = 59999;

    /** */
    private static int mcastPort = default_mcast_port;

    /** */
    private static int discoPort = default_disco_port;

    /** */
    private static int commPort = default_comm_port;

    /**
     * Initializes address.
     */
    static {
        InetAddress locHost = null;

        try {
            locHost = U.getLocalHost();
        }
        catch (IOException e) {
            assert false : "Unable to get local address. This leads to the same multicast addresses " +
                "in the local network.";
        }

        if (locHost != null) {
            int thirdByte = locHost.getAddress()[3];

            if (thirdByte < 0)
                thirdByte += 256;

            // To get different addresses for different machines.
            addr = new int[] {229, thirdByte, 1, 1};
        }
        else
            addr = new int[] {229, 1, 1, 1};
    }


    /**
     * Every invocation of this method will never return a
     * repeating communication port for a different test case.
     *
     * @param cls Class.
     * @return Next communication port.
     */
    public static synchronized int getNextCommPort(Class<?> cls) {
        Integer portRet = commPorts.get(cls);

        if (portRet != null)
            return portRet;

        if (commPort >= max_comm_port)
            commPort = default_comm_port;
        else
            // Reserve 10 ports per test.
            commPort += 10;

        portRet = commPort;

        // Cache port to be reused by the same test.
        commPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * Every invocation of this method will never return a
     * repeating discovery port for a different test case.
     *
     * @param cls Class.
     * @return Next discovery port.
     */
    public static synchronized int getNextDiscoPort(Class<?> cls) {
        Integer portRet = discoPorts.get(cls);

        if (portRet != null)
            return portRet;

        if (discoPort >= max_disco_port)
            discoPort = default_disco_port;
        else
            discoPort += 10;

        portRet = discoPort;

        // Cache port to be reused by the same test.
        discoPorts.put(cls, portRet);

        return portRet;
    }


    /**
     * Every invocation of this method will never return a
     * repeating multicast port for a different test case.
     *
     * @param cls Class.
     * @return Next multicast port.
     */
    public static synchronized int getNextMulticastPort(Class<?> cls) {
        Integer portRet = mcastPorts.get(cls);

        if (portRet != null)
            return portRet;

        int startPort = mcastPort;

        while (true) {
            if (mcastPort >= max_mcast_port)
                mcastPort = default_mcast_port;
            else
                mcastPort++;

            if (startPort == mcastPort)
                break;

            portRet = mcastPort;

            MulticastSocket sock = null;

            try {
                sock = new MulticastSocket(portRet);

                break;
            }
            catch (IOException ignored) {
                // No-op.
            }
            finally {
                U.closeQuiet(sock);
            }
        }

        // Cache port to be reused by the same test.
        mcastPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * @return Free communication port number on localhost.
     * @throws IOException If unable to find a free port.
     */
    public static int getFreeCommPort() throws IOException {
        for (int port = default_comm_port; port < max_comm_port; port++) {
            try (ServerSocket sock = new ServerSocket(port)) {
                return sock.getLocalPort();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }

        throw new IOException("Unable to find a free communication port.");
    }

    /**
     * Every invocation of this method will never return a
     * repeating multicast group for a different test case.
     *
     * @param cls Class.
     * @return Next multicast group.
     */
    public static synchronized String getNextMulticastGroup(Class<?> cls) {
        String addrStr = addrs.get(cls);

        if (addrStr != null)
            return addrStr;

        // Increment address.
        if (addr[3] == 255) {
            if (addr[2] == 255)
                assert false;
            else {
                addr[2] += 1;

                addr[3] = 1;
            }
        }
        else
            addr[3] += 1;

        // Convert address to string.
        StringBuilder b = new StringBuilder(15);

        for (int i = 0; i < addr.length; i++) {
            b.append(addr[i]);

            if (i < addr.length - 1)
                b.append('.');
        }

        addrStr = b.toString();

        // Cache address to be reused by the same test.
        addrs.put(cls, addrStr);

        return addrStr;
    }
}

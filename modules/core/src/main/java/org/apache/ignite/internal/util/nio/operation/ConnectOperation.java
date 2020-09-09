package org.apache.ignite.internal.util.nio.operation;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

public class ConnectOperation implements Runnable {
    /** */
    public static final int CONNECT_OP_META = GridNioSessionMetaKey.nextUniqueKey();

    private final GridNioServer<?> nioSrv;
    private final GridCommunicationClient parent;
    private final ScheduledExecutorService executor;
    private final List<InetSocketAddress> addresses;
    private final List<InetSocketAddress> failedAddresses = new ArrayList<>();
    private final UUID nodeId;
    private final int connIdx;

    private int idx;

    protected ConnectOperation(GridNioServer<?> nioSrv,
        GridCommunicationClient parent,
        List<InetSocketAddress> addresses, UUID nodeId, int connIdx,
        ScheduledExecutorService executor) {
        this.nioSrv = nioSrv;
        this.parent = parent;
        this.addresses = addresses;
        this.nodeId = nodeId;
        this.connIdx = connIdx;
        this.executor = executor;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        for (;idx < addresses.size();idx++) {
            InetSocketAddress addr = addresses.get(idx);

            if (addr.isUnresolved()) {
                failedAddresses.add(addr);
                continue;
            }

            SocketChannel ch = SocketChannel.open();
        }
    }

    /** */
    public void schedule() {
        schedule(0);
    }

    /** */
    public void schedule(long delay) {
        assert executor != null;
        assert delay >= 0;

        // todo only one operation at the same moment

        if (delay == 0)
            executor.execute(this);
        else
            executor.schedule(this, delay, TimeUnit.MILLISECONDS);
    }

    /** */
    private Map<Integer, Object> meta() {
        HashMap<Integer, Object> meta = new HashMap<>();
        meta.put(CONNECT_OP_META, this);
        meta.put(GridCommunicationClient.CLIENT_META, parent);
        meta.put(TcpCommunicationSpi.CONN_IDX_META, ConnectionKey.newOutgoingKey(nodeId, connIdx));

        return meta;
    }

    /** */
    private void connect(SocketChannel ch) {
        nioSrv.createSession(ch, meta(), true, this::onConnect);
    }

    /** */
    private void onConnect(IgniteInternalFuture<GridNioSession> f) {
        assert f.isDone();
        if (f.error() != null)
            schedule(1000); // retry;
    }
}

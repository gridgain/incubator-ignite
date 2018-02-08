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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AuthenticationConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.AUTH_PROC;

/**
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Store user prefix. */
    private static final String STORE_USER_PREFIX = "user.";

    /** Store users info version. */
    private static final String STORE_USERS_VERSION = "users-version";

    /** Discovery event types. */
    private static final int[] DISCO_EVT_TYPES = new int[] {EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_NODE_JOINED};

    /** User operation finish futures (Operation ID -> future). */
    private final ConcurrentMap<IgniteUuid, UserOperationFinishFuture> opFinishFuts
        = new ConcurrentHashMap<>();

    /** Futures prepared user map. Authentication message ID -> public future. */
    private final ConcurrentMap<IgniteUuid, GridFutureAdapter<User>> authFuts = new ConcurrentHashMap<>();
    /**
     * Mutex used to synchronize discovery thread on collect data bag
     * and change users from the thread of 'exec' executor.
     */
    private final Object muxChangeUserVer = new Object();

    /** Mutex for change coordinator node. */
    private final Object muxCrdNode = new Object();

    /** Task to put on auth executor queue to complete readyFut. */
    private final Runnable readyFutCompletor;

    /** Active operations. */
    private List<UserManagementOperation> activeOperations = Collections.synchronizedList(new ArrayList<>());

    /** User map. */
    private ConcurrentMap<String, User> users;

    /** Users info version. */
    private long usersInfoVer;

    /** Shared context. */
    @GridToStringExclude
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** Meta storage. */
    private ReadWriteMetastorage metastorage;

    /** Executor. */
    private IgniteThreadPoolExecutor exec;

    /** Coordinator node. */
    private ClusterNode crdNode;

    /** Is authentication enabled. */
    private boolean isEnabled;

    /** Disconnected flag. */
    private volatile boolean disconnected;

    /** Pending message of the finished operation. May be resend when coordinator node leave. */
    private UserManagementOperationFinishedMessage pendingFinishMsg;

    /** Initial users map and operations received from coordinator on the node joined to the cluster. */
    private InitialUsersData initUsrs;

    /** I/O message listener. */
    private GridMessageListener ioLsnr;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** Node activate future. */
    private GridFutureAdapter<Void> activateFut = new GridFutureAdapter<>();

    /** Authentication process is ready (initial users exchange is finished after cluster activate). */
    private GridFutureAdapter<Void> readyFut = new GridFutureAdapter<>();

    /**
     * @param ctx Kernal context.
     */
    public IgniteAuthenticationProcessor(GridKernalContext ctx) {
        super(ctx);

        AuthenticationConfiguration authCfg
            = ctx.config().getClientConnectorConfiguration().getAuthenticationConfiguration();

        isEnabled = authCfg != null && authCfg.isEnabled();

        readyFutCompletor = new Runnable() {
            @Override public void run() {
//                log.info("+++ READY");
                readyFut.onDone();
            }
        };

        if (!isEnabled)
            return;

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /**
     * @param n Node.
     * @return {@code true} if node holds user information. Otherwise returns {@code false}.
     */
    private static boolean isNodeHoldsUsers(ClusterNode n) {
        return !n.isClient() && !n.isDaemon();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        if (!isEnabled)
            return;

        GridDiscoveryManager discoMgr = ctx.discovery();

        GridIoManager ioMgr = ctx.io();

        discoMgr.setCustomEventListener(UserProposedMessage.class, new UserProposedListener());

        discoMgr.setCustomEventListener(UserAcceptedMessage.class, new UserAcceptedListener());

        discoLsnr = new DiscoveryEventListener() {
            @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
                if (ctx.isStopping())
                    return;

                switch (evt.type()) {
                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                        onNodeLeft(evt.eventNode().id());
                        break;

                    case EVT_NODE_JOINED:
                        onNodeJoin(evt.eventNode());
                        break;
                }
            }
        };

        ctx.event().addDiscoveryEventListener(discoLsnr, DISCO_EVT_TYPES);

        ioLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof UserManagementOperationFinishedMessage)
                    onFinishMessage(nodeId, (UserManagementOperationFinishedMessage)msg);
                else if (msg instanceof UserAuthenticateRequestMessage)
                    onAuthenticateRequestMessage(nodeId, (UserAuthenticateRequestMessage)msg);
                else if (msg instanceof UserAuthenticateResponseMessage)
                    onAuthenticateResponseMessage((UserAuthenticateResponseMessage)msg);
            }
        };

        ioMgr.addMessageListener(GridTopic.TOPIC_AUTH, ioLsnr);

        exec = new IgniteThreadPoolExecutor(
            "auth",
            ctx.config().getIgniteInstanceName(),
            1,
            1,
            0,
            new LinkedBlockingQueue<>());

        if (!GridCacheUtils.isPersistenceEnabled(ctx.config()))
            activateFut.onDone();
    }

    /**
     * On cache processor started.
     */
    public void cacheProcessorStarted() {
        sharedCtx = ctx.cache().context();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (!isEnabled)
            return;

        ctx.io().removeMessageListener(GridTopic.TOPIC_AUTH, ioLsnr);

        ctx.event().removeDiscoveryEventListener(discoLsnr, DISCO_EVT_TYPES);

        cancelFutures("Node stopped");

        if (!cancel)
            exec.shutdown();
        else
            exec.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (!isEnabled)
            return;

        cancelFutures("Kernal stopped.");
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        if (!isEnabled)
            return;

        assert !disconnected;

        disconnected = true;

        cancelFutures("Client node was disconnected from topology (operation result is unknown).");
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean active) {
        assert disconnected;

        disconnected = false;

        return null;
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object on successful authenticate. Otherwise returns {@code null}.
     * @throws IgniteCheckedException On authentication error.
     */
    public AuthorizationContext authenticate(String login, String passwd) throws IgniteCheckedException {
        checkActivate();
        checkEnabled();

        if (F.isEmpty(login))
            return null;

        if (ctx.clientNode()) {
            while (true) {
                try {
                    GridFutureAdapter<User> fut = new GridFutureAdapter<>();

                    UserAuthenticateRequestMessage msg = new UserAuthenticateRequestMessage(login, passwd);

                    authFuts.put(msg.id(), fut);

                    ctx.io().sendToGridTopic(coordinator(), GridTopic.TOPIC_AUTH, msg, GridIoPolicy.SYSTEM_POOL);

                    return new AuthorizationContext(fut.get());
                }
                catch (RetryOnCoordinatorLeftException e) {
                    // No-op.
                }
            }
        }
        else
            return new AuthorizationContext(authenticateOnServer(login, passwd));
    }

    /**
     * Adds new user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @throws IgniteAuthenticationException On error.
     */
    public void addUser(String login, String passwd) throws IgniteCheckedException {
        checkActivate();
        checkEnabled();

        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.ADD);

        execUserOperation(op);
    }

    /**
     * @param login User name.
     * @throws IgniteCheckedException On error.
     */
    public void removeUser(String login) throws IgniteCheckedException {
        checkActivate();
        checkEnabled();

        UserManagementOperation op = new UserManagementOperation(User.create(login),
            UserManagementOperation.OperationType.REMOVE);

        execUserOperation(op);
    }

    /**
     * @param login User name.
     * @param passwd User password.
     * @throws IgniteCheckedException On error.
     */
    public void updateUser(String login, String passwd) throws IgniteCheckedException {
        checkActivate();
        checkEnabled();

        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.UPDATE);

        execUserOperation(op);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        if (!ctx.clientNode()) {
            if (usersInfoVer != 0)
                return;

            Long ver = (Long)metastorage.read(STORE_USERS_VERSION);
            usersInfoVer = ver == null ? 0 : ver;

            users = new ConcurrentHashMap<>();

            Map<String, User> readUsers = (Map<String, User>)metastorage.readForPredicate(new IgnitePredicate<String>() {
                @Override public boolean apply(String key) {
                    return key != null && key.startsWith(STORE_USER_PREFIX);
                }
            });

            for (User u : readUsers.values())
                users.put(u.name(), u);
        }
        else
            users = null;
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        if (!ctx.clientNode())
            this.metastorage = metastorage;
        else
            this.metastorage = null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.AUTH_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        synchronized (muxChangeUserVer) {
            if (!dataBag.commonDataCollectedFor(AUTH_PROC.ordinal())) {
                InitialUsersData d = new InitialUsersData(users.values(), activeOperations, usersInfoVer);

                if (log.isDebugEnabled())
                    log.debug("Collected initial users data: " + d);

                dataBag.addGridCommonData(AUTH_PROC.ordinal(), d);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        initUsrs = (InitialUsersData)data.commonData();
    }

    /**
     * @return {@code true} if authentication is enabled, {@code false} if not.
     */
    public boolean enabled() {
        return isEnabled;
    }

    /**
     * Check cluster state.
     */
    private void checkActivate() {
        if (!ctx.state().publicApiActiveState(true)) {
            throw new IgniteException("Can not perform the operation because the cluster is inactive. Note, that " +
                "the cluster is considered inactive by default if Ignite Persistent Store is used to let all the nodes " +
                "join the cluster. To activate the cluster call Ignite.active(true).");
        }
    }

    /**
     */
    private void addDefaultUser() {
        assert usersInfoVer == 0;
        assert users != null && users.isEmpty();

        UserManagementOperation op = new UserManagementOperation(User.defaultUser(), UserManagementOperation.OperationType.ADD);

        activeOperations.add(op);

        UserOperationFinishFuture fut = new UserOperationFinishFuture(op, false);

        opFinishFuts.put(op.id(), fut);

        exec.execute(new UserOperationWorker(op, fut));
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object on successful authenticate. Otherwise returns {@code null}.
     * @throws IgniteCheckedException On authentication error.
     */
    private User authenticateOnServer(String login, String passwd) throws IgniteCheckedException {
        assert !ctx.clientNode() : "Must be used on server node";

        waitReady();

        User usr;

        usr = users.get(login);

        if (usr == null)
            throw new UserAuthenticationException("The user name or password is incorrect. [userName=" + login + ']');

        if (usr.authorize(passwd))
            return usr;
        else
            throw new UserAuthenticationException("The user name or password is incorrect. [userName=" + login + ']');
    }

    /**
     * @param op User operation.
     * @throws IgniteCheckedException On error.
     */
    private void execUserOperation(UserManagementOperation op) throws IgniteCheckedException {
        AuthorizationContext actx = AuthorizationContext.context();

        if (actx == null)
            throw new IgniteAccessControlException("Operation not allowed: authorized context is empty.");

        actx.checkUserOperation(op);

        UserOperationFinishFuture fut = new UserOperationFinishFuture(op, false);

        opFinishFuts.putIfAbsent(op.id(), fut);

        UserProposedMessage msg = new UserProposedMessage(op);

        ctx.discovery().sendCustomEvent(msg);

        fut.get();
    }

    /**
     * @param op The operation with users.
     * @throws IgniteAuthenticationException On authentication error.
     */
    private void processOperationLocal(UserManagementOperation op) throws IgniteCheckedException {
        assert op != null && op.user() != null : "Invalid operation: " + op;

//        log.info("+++ DO " + op);

        switch (op.type()) {
            case ADD:
                addUserLocal(op);

                break;

            case REMOVE:
                removeUserLocal(op);

                break;

            case UPDATE:
                updateUserLocal(op);

                break;
        }
    }

    /**
     * Adds new user locally.
     *
     * @param op User operation.
     * @throws IgniteAuthenticationException On error.
     */
    private void addUserLocal(final UserManagementOperation op) throws IgniteCheckedException {
        User usr = op.user();

        String userName = usr.name();

        if (users.containsKey(userName))
            throw new IgniteAuthenticationException("User already exists. [login=" + userName + ']');

        metastorage.write(STORE_USER_PREFIX + userName, usr);

        metastorage.write(STORE_USERS_VERSION, usersInfoVer + 1);

        synchronized (muxChangeUserVer) {
            usersInfoVer++;

            activeOperations.remove(op);

            users.put(userName, usr);
        }
    }

    /**
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     * @throws IgniteCheckedException On error.
     */
    private void removeUserLocal(UserManagementOperation op) throws IgniteCheckedException {
        User usr = op.user();

        if (!users.containsKey(usr.name()))
            throw new IgniteAuthenticationException("User doesn't exist. [userName=" + usr.name() + ']');

        metastorage.remove(STORE_USER_PREFIX + usr.name());

        metastorage.write(STORE_USERS_VERSION, usersInfoVer + 1);

        synchronized (muxChangeUserVer) {
            usersInfoVer++;

            activeOperations.remove(op);

            users.remove(usr.name());
        }
    }

    /**
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     * @throws IgniteCheckedException On error.
     */
    private void updateUserLocal(UserManagementOperation op) throws IgniteCheckedException {
        User usr = op.user();

        if (!users.containsKey(usr.name()))
            throw new IgniteAuthenticationException("User doesn't exist. [userName=" + usr.name() + ']');

        metastorage.write(STORE_USER_PREFIX + usr.name(), usr);

        metastorage.write(STORE_USERS_VERSION, usersInfoVer + 1);

        synchronized (muxChangeUserVer) {
            usersInfoVer++;

            activeOperations.remove(op);

            users.put(usr.name(), usr);
        }
    }

    /**
     *
     */
    private void checkEnabled() {
        if (!isEnabled) {
            throw new IgniteException("Can not perform the operation because the authentication" +
                " is not enabled for the cluster.");
        }
    }

    /**
     * Get current coordinator node.
     *
     * @return Coordinator node.
     */
    private ClusterNode coordinator() {
        synchronized (muxCrdNode) {
            if (crdNode != null)
                return crdNode;
            else {
                ClusterNode res = null;

                for (ClusterNode node : ctx.discovery().aliveServerNodes()) {
                    if (res == null || res.order() > node.order())
                        res = node;
                }

                assert res != null;

                crdNode = res;

                return res;
            }
        }
    }

    /**
     * @param msg Error message.
     */
    private void cancelFutures(String msg) {
        for (UserOperationFinishFuture fut : opFinishFuts.values())
            fut.onDone(null, new IgniteFutureCancelledException(msg));

        for (GridFutureAdapter<User> fut : authFuts.values())
            fut.onDone(null, new IgniteFutureCancelledException(msg));
    }

    /**
     * @param node Joined node ID.
     */
    private void onNodeJoin(ClusterNode node) {
        if (isNodeHoldsUsers(ctx.discovery().localNode()) && isNodeHoldsUsers(node)) {
            for (UserOperationFinishFuture f : opFinishFuts.values())
                f.onNodeJoin(node.id());
        }
    }

    /**
     * @param nodeId Left node ID.
     */
    private void onNodeLeft(UUID nodeId) {
        synchronized (muxCrdNode) {
            if (!ctx.clientNode()) {
                for (UserOperationFinishFuture f : opFinishFuts.values())
                    f.onNodeLeft(nodeId);
            }

            // Coordinator left
            if (F.eq(coordinator().id(), nodeId)) {
                for (GridFutureAdapter<User> f : authFuts.values())
                    f.onDone(new RetryOnCoordinatorLeftException());

                crdNode = null;

                if (pendingFinishMsg != null)
                    sendFinish(pendingFinishMsg);
            }
        }
    }

    /**
     * Handle finish operation message from a node.
     *
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void onFinishMessage(UUID nodeId, UserManagementOperationFinishedMessage msg) {
        if (log.isDebugEnabled())
            log.debug(msg.toString());

        UserOperationFinishFuture fut = opFinishFuts.get(msg.operationId());

        if (fut == null)
            log.warning("Not found appropriate user operation. [msg=" + msg + ']');
        else {
            if (msg.success())
                fut.onSuccessOnNode(nodeId);
            else
                fut.onOperationFailOnNode(nodeId, msg.errorMessage());
        }
    }

    /**
     * Called when all required finish messages are received,
     * send ACK message to complete operation futures on all nodes.
     *
     * @param opId Operation ID.
     * @param err Error.
     */
    private void onFinishOperation(IgniteUuid opId, IgniteCheckedException err) {
        try {
            UserAcceptedMessage msg = new UserAcceptedMessage(opId, err);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            if (!e.hasCause(IgniteFutureCancelledException.class))
                U.error(log, "Unexpected exception on send UserAcceptedMessage.", e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void onAuthenticateRequestMessage(UUID nodeId, UserAuthenticateRequestMessage msg) {
        UserAuthenticateResponseMessage respMsg;
        try {
            User u = authenticateOnServer(msg.name(), msg.password());

            respMsg = new UserAuthenticateResponseMessage(msg.id(), u, null);
        }
        catch (IgniteCheckedException e) {
            respMsg = new UserAuthenticateResponseMessage(msg.id(), null, e.toString());

            e.printStackTrace();
        }

        try {
            ctx.io().sendToGridTopic(nodeId, GridTopic.TOPIC_AUTH, respMsg, GridIoPolicy.SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Unexpected exception on send UserAuthenticateResponseMessage.", e);
        }
    }

    /**
     * @param msg Message.
     */
    private void onAuthenticateResponseMessage(UserAuthenticateResponseMessage msg) {
        GridFutureAdapter<User> fut = authFuts.get(msg.id());

        fut.onDone(msg.user(), !msg.success() ? new UserAuthenticationException(msg.errorMessage()) : null);
    }

    /**
     * Local node joined to topology. Discovery cache is available but no discovery custom message are received.
     * Initial user set and initial user operation (received on join) are processed here.
     *
     * @param evt Disco event.
     * @param cache Disco cache.
     */
    public void onLocalJoin(DiscoveryEvent evt, DiscoCache cache) {
        if (!isEnabled || ctx.clientNode()) {
            readyFut.onDone();

            return;
        }

        if (coordinator().id().equals(ctx.localNodeId())) {
            assert initUsrs == null;

            // Creates default user on coordinator if it is the first start of PDS cluster
            // or start of in-memory cluster.
            if (usersInfoVer == 0)
                addDefaultUser();
            else
                readyFut.onDone();
        }
        else {
            assert initUsrs != null;

            // Can be empty on initial start of PDS cluster (default user will be created and stored after activate)
            if (!F.isEmpty(initUsrs.usrs))
                exec.execute(new RefreshUsersStorageWorker(initUsrs.usrs));

            for (UserManagementOperation op : initUsrs.activeOps) {
                UserOperationFinishFuture fut = new UserOperationFinishFuture(op, true);

                opFinishFuts.put(op.id(), fut);

                exec.execute(new UserOperationWorker(op, fut));
            }

            usersInfoVer = initUsrs.usrVer;
        }

        exec.execute(readyFutCompletor);
    }

    /**
     * Called on node activate.
     */
    public void onActivate() {
        activateFut.onDone();
    }

    /**
     *
     */
    private void waitActivate() {
        try {
            activateFut.get();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }
    }

    /**
     *
     */
    private void waitReady() {
        try {
            readyFut.get();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }
    }

    /**
     * @param msg Finish message.
     */
    private void sendFinish(UserManagementOperationFinishedMessage msg) {
        try {
//            log.info("+++ SEND FINISH " + msg);
            ctx.io().sendToGridTopic(coordinator(), GridTopic.TOPIC_AUTH, msg, GridIoPolicy.SYSTEM_POOL);
        }
        catch (Exception e) {
            U.error(log, "Failed to send UserManagementOperationFinishedMessage. [op=" + msg.operationId() +
                ", node=" + coordinator() + ", err=" + msg.errorMessage() + ']', e);
        }
    }

    /**
     * Initial data is collected on coordinator to send to join node.
     */
    private static final class InitialUsersData implements Serializable {
        /** Users. */
        private final ArrayList<User> usrs;

        /** Active user operations. */
        private final ArrayList<UserManagementOperation> activeOps;

        /** Users information version. */
        private final long usrVer;

        /**
         * @param usrs Users.
         * @param ops Active operations on cluster.
         * @param ver Users info version.
         */
        InitialUsersData(Collection<User> usrs, List<UserManagementOperation> ops, long ver) {
            this.usrs = new ArrayList<>(usrs);
            activeOps = new ArrayList<>(ops);
            usrVer = ver;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(InitialUsersData.class, this);
        }
    }

    /**
     * Thrown by authenticate futures when coordinator node left
     * to resend authenticate message to the new coordinator.
     */
    private static class RetryOnCoordinatorLeftException extends IgniteCheckedException {
    }

    /**
     *
     */
    private final class UserProposedListener implements CustomEventListener<UserProposedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            final UserProposedMessage msg) {
            if (ctx.isStopping() || ctx.clientNode())
                return;

//          log.info("+++ PROPOSE " + msg.operation());
            if (log.isDebugEnabled())
                log.debug(msg.toString());

            UserManagementOperation op = msg.operation();

            UserOperationFinishFuture fut = new UserOperationFinishFuture(op, true);
            UserOperationFinishFuture futPrev = opFinishFuts.putIfAbsent(op.id(), fut);

            if (futPrev == null) {
                activeOperations.add(op);

                exec.execute(new UserOperationWorker(msg.operation(), fut));
            }
            else if (!futPrev.workerSubmitted()) {
                activeOperations.add(op);

                exec.execute(new UserOperationWorker(msg.operation(), futPrev));
            }
        }
    }

    /**
     *
     */
    private final class UserAcceptedListener implements CustomEventListener<UserAcceptedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, UserAcceptedMessage msg) {
            if (log.isDebugEnabled())
                log.debug(msg.toString());

//            log.info("+++ ACK " + msg.operationId());
            UserOperationFinishFuture f = opFinishFuts.get(msg.operationId());

            if (f != null) {
                if (msg.error() != null)
                    f.onDone(null, msg.error());
                else
                    f.onDone();
            }
        }
    }

    /**
     * Future to wait for end of user operation. Removes itself from map when completed.
     */
    private class UserOperationFinishFuture extends GridFutureAdapter<Void> {
        /** */
        private final Set<UUID> requiredFinish;

        /** */
        private final Set<UUID> receivedFinish;

        /** User management operation. */
        private final UserManagementOperation op;

        /** Worker has been already submitted flag. */
        private final boolean workerSubmitted;

        /** Error. */
        private IgniteCheckedException err;

        /**
         * @param op User management operation.
         * @param workerSubmitted Worker has been already submitted flag.
         */
        UserOperationFinishFuture(UserManagementOperation op, boolean workerSubmitted) {
            this.op = op;
            this.workerSubmitted = workerSubmitted;

            if (!ctx.clientNode()) {
                requiredFinish = new HashSet<>();
                receivedFinish = new HashSet<>();

                for (ClusterNode node : ctx.discovery().nodes(ctx.discovery().topologyVersionEx())) {
                    if (isNodeHoldsUsers(node))
                        requiredFinish.add(node.id());
                }
            }
            else {
                requiredFinish = null;
                receivedFinish = null;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            boolean done = super.onDone(res, err);

            if (done)
                opFinishFuts.remove(op.id(), this);

            return done;
        }

        /**
         * @param nodeId ID of left or failed node.
         */
        synchronized void onNodeLeft(UUID nodeId) {
            assert requiredFinish != null : "Process node left on client";

            requiredFinish.remove(nodeId);

            checkOperationFinished();
        }

        /**
         * @param id Joined node ID.
         */
        synchronized void onNodeJoin(UUID id) {
            assert requiredFinish != null : "Process node join on client";

            requiredFinish.add(id);
        }

        /**
         * @param nodeId Node ID.
         */
        synchronized void onSuccessOnNode(UUID nodeId) {
            assert receivedFinish != null : "Process operation state on client";

            receivedFinish.add(nodeId);

            checkOperationFinished();
        }

        /**
         * @param nodeId Node ID.
         * @param errMsg Error message.
         */
        synchronized void onOperationFailOnNode(UUID nodeId, String errMsg) {
            assert receivedFinish != null : "Process operation state on client";

            if (log.isDebugEnabled())
                log.debug("User operation is failed. [nodeId=" + nodeId + ", err=" + errMsg + ']');

            receivedFinish.add(nodeId);

            UserAuthenticationException e = new UserAuthenticationException("Operation failed. [nodeId=" + nodeId
                + ", op=" + op + ", err=" + errMsg + ']');

            if (err == null)
                err = e;
            else
                err.addSuppressed(e);

            checkOperationFinished();
        }

        /**
         *
         */
        private void checkOperationFinished() {
//            log.info("+++ CHECK F req="  + requiredFinish.size() + ", revc=" + receivedFinish.size());
            if (receivedFinish.containsAll(requiredFinish))
                onFinishOperation(op.id(), err);
        }

        /**
         * @return Worker submitted flag.
         */
        public boolean workerSubmitted() {
            return workerSubmitted;
        }
    }

    /**
     * User operation worker.
     */
    private class UserOperationWorker extends GridWorker {
        /** User operation. */
        private final UserManagementOperation op;

        /** Operation future. */
        private final UserOperationFinishFuture fut;

        /**
         * Constructor.
         *
         * @param op Operation.
         * @param fut Operation finish future.
         */
        private UserOperationWorker(UserManagementOperation op, UserOperationFinishFuture fut) {
            super(ctx.igniteInstanceName(), "auth-op-" + op.type(), IgniteAuthenticationProcessor.this.log);

            this.op = op;
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (ctx.isStopping())
                return;

            waitActivate();

            UserManagementOperationFinishedMessage msg0 = null;

            if (sharedCtx != null)
                sharedCtx.database().checkpointReadLock();

            try {
                processOperationLocal(op);

                msg0 = new UserManagementOperationFinishedMessage(op.id(), null);
            }
            catch (Throwable e) {
                msg0 = new UserManagementOperationFinishedMessage(op.id(), e.toString());
            }
            finally {
                assert msg0 != null;

                if (sharedCtx != null)
                    sharedCtx.database().checkpointReadUnlock();

                pendingFinishMsg = msg0;

                sendFinish(pendingFinishMsg);
            }

            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                if (!e.hasCause(IgniteFutureCancelledException.class))
                    U.error(log, "Unexpected exception on wait for end of user operation.", e);
            }
            finally {
                pendingFinishMsg = null;
            }
        }
    }

    /**
     * Initial users set worker.
     */
    private class RefreshUsersStorageWorker extends GridWorker {
        private final ArrayList<User> newUsrs;

        /**
         * @param usrs New users to store.
         */
        private RefreshUsersStorageWorker(ArrayList<User> usrs) {
            super(ctx.igniteInstanceName(), "refresh-store", IgniteAuthenticationProcessor.this.log);

            assert !F.isEmpty(usrs);

            newUsrs = usrs;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (ctx.clientNode())
                return;

            waitActivate();

            if (sharedCtx != null)
                sharedCtx.database().checkpointReadLock();

            try {
                Map<String, User> existUsrs = (Map<String, User>)metastorage.readForPredicate(new IgnitePredicate<String>() {
                    @Override public boolean apply(String key) {
                        return key != null && key.startsWith(STORE_USER_PREFIX);
                    }
                });

                for (String name : existUsrs.keySet())
                    metastorage.remove(name);

                users.clear();

                for (User u : newUsrs) {
                    metastorage.write(u.name(), u);

                    users.put(u.name(), u);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Cannot cleanup old users information at metastorage", e);
            }
            finally {
                if (sharedCtx != null)
                    sharedCtx.database().checkpointReadUnlock();
            }
        }
    }
}

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

import _ from 'lodash';
import {nonEmpty, nonNil} from 'app/utils/lodashMixins';

import Sockette from 'sockette';

import {BehaviorSubject, Subject} from 'rxjs';
import {distinctUntilChanged, filter, first, map, pluck, take, tap} from 'rxjs/operators';

import uuidv4 from 'uuid/v4';

import AgentModal from './AgentModal.service';
// @ts-ignore
import Worker from './decompress.worker';
import SimpleWorkerPool from '../../utils/SimpleWorkerPool';
import maskNull from 'app/core/utils/maskNull';

import {CancellationError} from 'app/errors/CancellationError';
import {ClusterSecretsManager} from './types/ClusterSecretsManager';
import ClusterLoginService from './components/cluster-login/service';

const __dbg = false;

const State = {
    INIT: 'INIT',
    AGENT_DISCONNECTED: 'AGENT_DISCONNECTED',
    CLUSTER_DISCONNECTED: 'CLUSTER_DISCONNECTED',
    CONNECTED: 'CONNECTED'
};

const IGNITE_2_0 = '2.0.0';
const LAZY_QUERY_SINCE = [['2.1.4-p1', '2.2.0'], '2.2.1'];
const COLLOCATED_QUERY_SINCE = [['2.3.5', '2.4.0'], ['2.4.6', '2.5.0'], ['2.5.1-p13', '2.6.0'], '2.7.0'];
const COLLECT_BY_CACHE_GROUPS_SINCE = '2.7.0';
const QUERY_PING_SINCE = [['2.5.6', '2.6.0'], '2.7.4'];

/**
 * Query execution result.
 * @typedef {{responseNodeId: String, queryId: String, columns: String[], rows: {Object[][]}, hasMore: Boolean, duration: Number}} VisorQueryResult
 */

/**
 * Query ping result.
 * @typedef {{}} VisorQueryPingResult
 */

/** Reserved cache names */
const RESERVED_CACHE_NAMES = [
    'ignite-hadoop-mr-sys-cache',
    'ignite-sys-cache',
    'MetaStorage',
    'TxLog'
];

/** Error codes from o.a.i.internal.processors.restGridRestResponse.java */
const SuccessStatus = {
    /** Command succeeded. */
    STATUS_SUCCESS: 0,
    /** Command failed. */
    STATUS_FAILED: 1,
    /** Authentication failure. */
    AUTH_FAILED: 2,
    /** Security check failed. */
    SECURITY_CHECK_FAILED: 3
};

class ConnectionState {
    constructor(cluster) {
        this.cluster = cluster;
        this.clusters = [];
        this.state = State.INIT;
    }

    updateCluster(cluster) {
        this.cluster = cluster;
        this.cluster.connected = !!_.find(this.clusters, {id: this.cluster.id});

        return cluster;
    }

    update(demo, count, clusters, hasDemo) {
        this.clusters = clusters;

        if (_.isEmpty(this.clusters))
            this.cluster = null;

        if (_.isNil(this.cluster))
            this.cluster = _.head(clusters);

        if (this.cluster)
            this.cluster.connected = !!_.find(clusters, {id: this.cluster.id});

        this.hasDemo = hasDemo;

        if (count === 0)
            this.state = State.AGENT_DISCONNECTED;
        else if (demo || _.get(this.cluster, 'connected'))
            this.state = State.CONNECTED;
        else
            this.state = State.CLUSTER_DISCONNECTED;
    }

    useConnectedCluster() {
        if (nonEmpty(this.clusters) && !this.cluster.connected) {
            this.cluster = _.head(this.clusters);

            this.cluster.connected = true;

            this.state = State.CONNECTED;
        }
    }

    disconnect() {
        if (this.cluster)
            this.cluster.disconnect = true;

        this.clusters = [];
        this.state = State.AGENT_DISCONNECTED;
    }
}

export default class AgentManager {
    static $inject = ['$rootScope', '$q', '$transitions', '$location', 'AgentModal', 'UserNotifications', 'IgniteVersion', 'ClusterLoginService'];

    /** Browser ID. */
    browserId = uuidv4();

    /** @type {ng.IScope} */
    $root;

    /** @type {ng.IQService} */
    $q;

    /** @type {AgentModal} */
    agentModal;

    /** @type {ClusterLoginService} */
    ClusterLoginSrv;

    /** @type {String} */
    clusterVersion;

    connectionSbj = new BehaviorSubject(new ConnectionState(AgentManager.restoreActiveCluster()));

    /** @type {ClusterSecretsManager} */
    clustersSecrets = new ClusterSecretsManager();

    pool = new SimpleWorkerPool('decompressor', Worker, 4);

    /** @type {Set<ng.IPromise<unknown>>} */
    promises = new Set();

    /** Websocket */
    ws = null;

    wsSubject = new Subject();

    _visorTasks = new Map();

    /** @type {Set<() => Promise>} */
    switchClusterListeners = new Set();

    addClusterSwitchListener(func) {
        this.switchClusterListeners.add(func);
    }

    removeClusterSwitchListener(func) {
        this.switchClusterListeners.delete(func);
    }

    static restoreActiveCluster() {
        try {
            return JSON.parse(localStorage.cluster);
        }
        catch (ignore) {
            return null;
        }
        finally {
            localStorage.removeItem('cluster');
        }
    }

    /**
     * @param {ng.IRootScopeService} $root
     * @param {ng.IQService} $q
     * @param {import('@uirouter/angularjs').TransitionService} $transitions
     * @param {ng.ILocationService} $location
     * @param {import('./AgentModal.service').default} agentModal
     * @param {import('app/components/user-notifications/service').default} UserNotifications
     * @param {import('app/services/Version.service').default} Version
     * @param {import('./components/cluster-login/service').default} ClusterLoginSrv
     */
    constructor($root, $q, $transitions, $location, agentModal, UserNotifications, Version, ClusterLoginSrv) {
        this.$root = $root;
        this.$q = $q;
        this.$transitions = $transitions;
        this.$location = $location;
        this.agentModal = agentModal;
        this.UserNotifications = UserNotifications;
        this.Version = Version;
        this.ClusterLoginSrv = ClusterLoginSrv;

        this.clusterVersion = this.Version.webConsole;

        let prevCluster;

        this.currentCluster$ = this.connectionSbj.pipe(
            distinctUntilChanged(({ cluster }) => prevCluster === cluster),
            tap(({ cluster }) => prevCluster = cluster)
        );

        this.clusterIsActive$ = this.connectionSbj.pipe(
            map(({ cluster }) => cluster),
            filter((cluster) => Boolean(cluster)),
            pluck('active')
        );

        this.clusterIsAvailable$ = this.connectionSbj.pipe(
            pluck('cluster'),
            map((cluster) => !!cluster)
        );

        if (!this.isDemoMode()) {
            this.connectionSbj.subscribe({
                next: ({cluster}) => {
                    const version = _.get(cluster, 'clusterVersion');

                    if (_.isEmpty(version))
                        return;

                    this.clusterVersion = version;
                }
            });
        }
    }

    // TODO WC-1030: Move to backend.
    registerVisorTask(taskId, taskCls, ...argCls) {
        this._visorTasks.set(taskId, {
            taskCls,
            argCls
        });
    }

    // TODO WC-1030: Move to backend.
    registerVisorTasks() {
        const internalVisor = (postfix) => `org.apache.ignite.internal.visor.${postfix}`;

        this.registerVisorTask('querySql', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryArg'));
        this.registerVisorTask('querySqlV2', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryArgV2'));
        this.registerVisorTask('querySqlV3', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryArgV3'));
        this.registerVisorTask('querySqlX2', internalVisor('query.VisorQueryTask'), internalVisor('query.VisorQueryTaskArg'));

        this.registerVisorTask('queryScanX2', internalVisor('query.VisorScanQueryTask'), internalVisor('query.VisorScanQueryTaskArg'));

        this.registerVisorTask('queryFetch', internalVisor('query.VisorQueryNextPageTask'), 'org.apache.ignite.lang.IgniteBiTuple', 'java.lang.String', 'java.lang.Integer');
        this.registerVisorTask('queryFetchX2', internalVisor('query.VisorQueryNextPageTask'), internalVisor('query.VisorQueryNextPageTaskArg'));

        this.registerVisorTask('queryFetchFirstPage', internalVisor('query.VisorQueryFetchFirstPageTask'), internalVisor('query.VisorQueryNextPageTaskArg'));

        this.registerVisorTask('queryClose', internalVisor('query.VisorQueryCleanupTask'), 'java.util.Map', 'java.util.UUID', 'java.util.Set');
        this.registerVisorTask('queryCloseX2', internalVisor('query.VisorQueryCleanupTask'), internalVisor('query.VisorQueryCleanupTaskArg'));

        this.registerVisorTask('toggleClusterState', internalVisor('misc.VisorChangeGridActiveStateTask'), internalVisor('misc.VisorChangeGridActiveStateTaskArg'));

        this.registerVisorTask('cacheNamesCollectorTask', internalVisor('cache.VisorCacheNamesCollectorTask'), 'java.lang.Void');

        this.registerVisorTask('cacheNodesTask', internalVisor('cache.VisorCacheNodesTask'), 'java.lang.String');
        this.registerVisorTask('cacheNodesTaskX2', internalVisor('cache.VisorCacheNodesTask'), internalVisor('cache.VisorCacheNodesTaskArg'));
    }

    isDemoMode() {
        return this.$root.IgniteDemoMode;
    }

    available(...sinceVersion) {
        return this.Version.since(this.clusterVersion, ...sinceVersion);
    }

    connect() {
        if (nonNil(this.ws))
            return;

        // TODO IGNITE-5617 support demo mode.
        // const options = this.isDemoMode() ? {query: 'IgniteDemoMode=true'} : {};

        const protocol = this.$location.protocol();
        const host = this.$location.host();
        const port = this.$location.port();

        const uri = `${protocol === 'https' ? 'wss' : 'ws'}://${host}:${port}/browsers`;

        // Open websocket connection to backend.
        this.ws = new Sockette(uri, {
            timeout: 5000, // Retry every 5 seconds
            onopen: (evt) => {
                if (__dbg)
                    console.log('[WS] Connected to server: ', evt);
            },
            onmessage: (msg) => {
                if (__dbg)
                    console.log('[WS] Received: ', msg);

                const evt = JSON.parse(msg.data);

                const eventType = evt.eventType;
                const payload = JSON.parse(evt.payload);

                if (eventType === 'agent:status') {
                    const {clusters, count, hasDemo} = payload;

                    const conn = this.connectionSbj.getValue();

                    conn.update(this.isDemoMode(), count, clusters, hasDemo);

                    this.connectionSbj.next(conn);
                }
                else if (eventType === 'cluster:changed')
                    this.updateCluster(payload);
                else if (eventType === 'user:notifications')
                    this.UserNotifications.notification = payload;
                else {
                    this.wsSubject.next({
                        requestId: evt.requestId,
                        eventType,
                        payload
                    });
                }
            },
            onreconnect: (evt) => {
                if (__dbg)
                    console.log('[WS] Reconnecting...', evt);
            },
            onclose: (evt) => {
                if (__dbg)
                    console.log('[WS] Disconnected from server: ', evt);

                const conn = this.connectionSbj.getValue();

                conn.disconnect();

                this.connectionSbj.next(conn);

                this.wsSubject.next({
                    requestId: 'any',
                    eventType: 'disconnected',
                    payload: 'none'
                });
            },
            onerror: (evt) => {
                if (__dbg)
                    console.log('[WS] Error on sending message to server: ', evt);
            }
        });
    }

    _sendWebSocketEvent(requestId, eventType, data) {
        this.ws.json({
            requestId,
            eventType,
            payload: JSON.stringify(data)
        });
    }

    saveToStorage(cluster = this.connectionSbj.getValue().cluster) {
        try {
            localStorage.cluster = JSON.stringify(cluster);
        }
        catch (ignore) {
            // No-op.
        }
    }

    updateCluster(newCluster) {
        const state = this.connectionSbj.getValue();

        const oldCluster = _.find(state.clusters, (cluster) => cluster.id === newCluster.id);

        if (!_.isNil(oldCluster)) {
            oldCluster.nids = newCluster.nids;
            oldCluster.addresses = newCluster.addresses;
            oldCluster.clusterVersion = newCluster.clusterVersion;
            oldCluster.active = newCluster.active;

            this.connectionSbj.next(state);
        }
    }

    switchCluster(cluster) {
        return Promise.all(_.map([...this.switchClusterListeners], (lnr) => lnr()))
            .then(() => {
                const state = this.connectionSbj.getValue();

                state.updateCluster(cluster);

                this.connectionSbj.next(state);

                this.saveToStorage(cluster);

                return Promise.resolve();
            });
    }

    /**
     * @param states
     * @returns {ng.IPromise}
     */
    awaitConnectionState(...states) {
        const defer = this.$q.defer();

        this.promises.add(defer);

        const subscription = this.connectionSbj.subscribe({
            next: ({state}) => {
                if (_.includes(states, state))
                    defer.resolve();
            }
        });

        return defer.promise
            .finally(() => {
                subscription.unsubscribe();

                this.promises.delete(defer);
            });
    }

    awaitCluster() {
        return this.awaitConnectionState(State.CONNECTED);
    }

    awaitAgent() {
        return this.awaitConnectionState(State.CONNECTED, State.CLUSTER_DISCONNECTED);
    }

    /**
     * Send message.
     *
     * @param {String} eventType
     * @param {Object} data
     * @returns {ng.IPromise}
     * @private
     */
    _sendToAgent(eventType, data = {}) {
        if (!this.ws)
            return this.$q.reject('Failed to connect to server');

        const latch = this.$q.defer();

        // Generate unique request ID in order to process response.
        const requestId = uuidv4();

        if (__dbg)
            console.log(`Sending request: ${eventType}, ${requestId}`);

        this.wsSubject
            .pipe(
                filter((evt) => evt.requestId === requestId || evt.eventType === 'disconnected'),
                take(1)
            )
            .toPromise()
            .then((evt) => {
                if (__dbg)
                    console.log('Received response: ', evt);

                if (evt.eventType === 'error')
                    latch.reject(evt.payload);
                else if (evt.eventType === 'disconnected')
                    latch.reject({message: 'Connection to web server was lost'});
                else
                    latch.resolve(evt.payload);
            });

        this._sendWebSocketEvent(requestId, eventType, data);

        return latch.promise;
    }

    drivers() {
        return this._sendToAgent('schemaImport:drivers');
    }

    /**
     * @param {{jdbcDriverJar: String, jdbcDriverClass: String, jdbcUrl: String, user: String, password: String}}
     * @returns {ng.IPromise}
     */
    schemas({jdbcDriverJar, jdbcDriverClass, jdbcUrl, user, password}) {
        const info = {user, password};

        return this._sendToAgent('schemaImport:schemas', {jdbcDriverJar, jdbcDriverClass, jdbcUrl, info});
    }

    /**
     * @param {{jdbcDriverJar: String, jdbcDriverClass: String, jdbcUrl: String, user: String, password: String, schemas: String, tablesOnly: String}}
     * @returns {ng.IPromise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
     */
    tables({jdbcDriverJar, jdbcDriverClass, jdbcUrl, user, password, schemas, tablesOnly}) {
        const info = {user, password};

        return this._sendToAgent('schemaImport:metadata', {jdbcDriverJar, jdbcDriverClass, jdbcUrl, info, schemas, tablesOnly});
    }

    /**
     * @param {Object} cluster
     * @param {Object} credentials
     * @param {String} event
     * @param {Object} params
     * @returns {ng.IPromise}
     * @private
     */
    _executeOnActiveCluster(cluster, credentials, event, params) {
        return this._sendToAgent(event, {clusterId: cluster.id, params, credentials})
            .then((res) => {
                const {status = SuccessStatus.STATUS_SUCCESS} = res;

                switch (status) {
                    case SuccessStatus.STATUS_SUCCESS:
                        if (cluster.secured)
                            this.clustersSecrets.get(cluster.id).sessionToken = res.sessionToken;

                        const taskId = _.get(params, 'taskId', '');

                        const useBigIntJson = taskId.startsWith('query');

                        return this.pool.postMessage({payload: res.data, useBigIntJson})
                            .then((data) => data.result ? data.result : data);

                    case SuccessStatus.STATUS_FAILED:
                        if (res.error.startsWith('Failed to handle request - unknown session token (maybe expired session)')) {
                            this.clustersSecrets.get(cluster.id).resetSessionToken();

                            return this._executeOnCluster(event, params);
                        }

                        throw new Error(res.error);

                    case SuccessStatus.AUTH_FAILED:
                        this.clustersSecrets.get(cluster.id).resetCredentials();

                        throw new Error('Cluster authentication failed. Incorrect user and/or password.');

                    case SuccessStatus.SECURITY_CHECK_FAILED:
                        throw new Error('Access denied. You are not authorized to access this functionality.');

                    default:
                        throw new Error('Illegal status in node response');
                }
            });
    }

    /**
     * @param {String} event
     * @param {Object} params
     * @returns {Promise}
     * @private
     */
    _executeOnCluster(event, params) {
        if (this.isDemoMode())
            return Promise.resolve(this._executeOnActiveCluster({}, {}, event, params));

        return this.connectionSbj.pipe(first()).toPromise()
            .then(({cluster}) => {
                if (_.isNil(cluster))
                    throw new Error('Failed to execute request on cluster.');

                if (cluster.secured) {
                    return Promise.resolve(this.clustersSecrets.get(cluster.id))
                        .then((secrets) => {
                            if (secrets.hasCredentials())
                                return secrets;

                            return this.ClusterLoginSrv.askCredentials(secrets)
                                .then((secrets) => {
                                    this.clustersSecrets.put(cluster.id, secrets);

                                    return secrets;
                                });
                        })
                        .then((secrets) => ({cluster, credentials: secrets.getCredentials()}));
                }

                return {cluster, credentials: {}};
            })
            .then(({cluster, credentials}) => this._executeOnActiveCluster(cluster, credentials, event, params))
            .catch((err) => {
                if (err instanceof CancellationError)
                    return;

                throw err;
            });
    }

    /**
     * @param {boolean} [attr] Collect node attributes.
     * @param {boolean} [mtr] Collect node metrics.
     * @param {boolean} [caches] Collect node caches descriptors.
     * @returns {Promise}
     */
    topology(attr = false, mtr = false, caches = false) {
        return this._executeOnCluster('node:rest', {cmd: 'top', attr, mtr, caches});
    }

    collectCacheNames(nid) {
        if (this.available(COLLECT_BY_CACHE_GROUPS_SINCE))
            return this.visorTask('cacheNamesCollectorTask', nid);

        return Promise.resolve({cacheGroupsNotAvailable: true});
    }

    publicCacheNames() {
        return this.collectCacheNames()
            .then((data) => {
                if (nonEmpty(data.caches))
                    return _.difference(_.keys(data.caches), RESERVED_CACHE_NAMES);

                return this.topology(false, false, true)
                    .then((nodes) => {
                        return _.map(_.uniqBy(_.flatMap(nodes, 'caches'), 'name'), 'name');
                    });
            });
    }

    /**
     * @param {string} cacheName Cache name.
     */
    cacheNodes(cacheName) {
        if (this.available(IGNITE_2_0))
            return this.visorTask('cacheNodesTaskX2', null, cacheName);

        return this.visorTask('cacheNodesTask', null, cacheName);
    }

    /**
     * @returns {Promise}
     */
    metadata() {
        return this._executeOnCluster('node:rest', {cmd: 'metadata'})
            .then((caches) => {
                let types = [];

                const _compact = (className) => {
                    return className.replace('java.lang.', '').replace('java.util.', '').replace('java.sql.', '');
                };

                const _typeMapper = (meta, typeName) => {
                    const maskedName = _.isEmpty(meta.cacheName) ? '<default>' : meta.cacheName;

                    let fields = meta.fields[typeName];

                    let columns = [];

                    for (const fieldName in fields) {
                        if (fields.hasOwnProperty(fieldName)) {
                            const fieldClass = _compact(fields[fieldName]);

                            columns.push({
                                type: 'field',
                                name: fieldName,
                                clazz: fieldClass,
                                system: fieldName === '_KEY' || fieldName === '_VAL',
                                cacheName: meta.cacheName,
                                typeName,
                                maskedName
                            });
                        }
                    }

                    const indexes = [];

                    for (const index of meta.indexes[typeName]) {
                        fields = [];

                        for (const field of index.fields) {
                            fields.push({
                                type: 'index-field',
                                name: field,
                                order: index.descendings.indexOf(field) < 0,
                                unique: index.unique,
                                cacheName: meta.cacheName,
                                typeName,
                                maskedName
                            });
                        }

                        if (fields.length > 0) {
                            indexes.push({
                                type: 'index',
                                name: index.name,
                                children: fields,
                                cacheName: meta.cacheName,
                                typeName,
                                maskedName
                            });
                        }
                    }

                    columns = _.sortBy(columns, 'name');

                    if (nonEmpty(indexes)) {
                        columns = columns.concat({
                            type: 'indexes',
                            name: 'Indexes',
                            cacheName: meta.cacheName,
                            typeName,
                            maskedName,
                            children: indexes
                        });
                    }

                    return {
                        type: 'type',
                        cacheName: meta.cacheName || '',
                        typeName,
                        maskedName,
                        children: columns
                    };
                };

                for (const meta of caches) {
                    const cacheTypes = meta.types.map(_typeMapper.bind(null, meta));

                    if (!_.isEmpty(cacheTypes))
                        types = types.concat(cacheTypes);
                }

                return types;
            });
    }

    /**
     * @param {String} taskId
     * @param {Array.<String>|String} nids
     * @param {Array.<Object>} args
     */
    visorTask(taskId, nids, ...args) {
        if (_.isEmpty(this._visorTasks))
            this.registerVisorTasks();

        const desc = this._visorTasks.get(taskId);

        if (_.isNil(desc))
            return Promise.reject(`Failed to find Visor task for ID: ${taskId}`);

        args = _.map(args, (arg) => maskNull(arg));

        nids = _.isArray(nids) ? nids.join(';') : maskNull(nids);

        const exeParams = {
            taskId,
            nids,
            cmd: 'exe',
            name: 'org.apache.ignite.internal.visor.compute.VisorGatewayTask',
            p1: nids,
            p2: desc.taskCls
        };

        _.forEach(_.concat(desc.argCls, args), (param, idx) => { exeParams[`p${idx + 3}`] = param; });

        return this._executeOnCluster('node:visor', exeParams);
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} enforceJoinOrder Flag whether enforce join order is enabled.
     * @param {Boolean} replicatedOnly Flag whether query contains only replicated tables.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {Number} pageSize
     * @param {Boolean} [lazy] query flag.
     * @param {Boolean} [collocated] Collocated query.
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    querySql({nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSize, lazy = false, collocated = false}) {
        if (this.available(IGNITE_2_0)) {
            let args = [cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSize];

            if (this.available(...COLLOCATED_QUERY_SINCE))
                args = [...args, lazy, collocated];
            else if (this.available(...LAZY_QUERY_SINCE))
                args = [...args, lazy];

            return this.visorTask('querySqlX2', nid, ...args).then(({error, result}) => {
                if (_.isEmpty(error))
                    return result;

                return Promise.reject(error);
            });
        }

        cacheName = _.isEmpty(cacheName) ? null : cacheName;

        let queryPromise;

        if (enforceJoinOrder)
            queryPromise = this.visorTask('querySqlV3', nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, local, pageSize);
        else if (nonCollocatedJoins)
            queryPromise = this.visorTask('querySqlV2', nid, cacheName, query, nonCollocatedJoins, local, pageSize);
        else
            queryPromise = this.visorTask('querySql', nid, cacheName, query, local, pageSize);

        return queryPromise
            .then(({key, value}) => {
                if (_.isEmpty(key))
                    return value;

                return Promise.reject(key);
            });
    }

    /**
     * @param {String} nid Node id.
     * @param {String} queryId Query ID.
     * @param {Number} pageSize
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    queryFetchFistsPage(nid, queryId, pageSize) {
        return this.visorTask('queryFetchFirstPage', nid, queryId, pageSize).then(({error, result}) => {
            if (_.isEmpty(error))
                return result;

            return Promise.reject(error);
        });
    }

    /**
     * @param {String} nid Node id.
     * @param {String} queryId Query ID.
     * @returns {Promise.<VisorQueryPingResult>} Query execution result.
     */
    queryPing(nid, queryId) {
        if (this.available(...QUERY_PING_SINCE)) {
            return this.visorTask('queryPing', nid, queryId, 1).then(({error, result}) => {
                if (_.isEmpty(error))
                    return {queryPingSupported: true};

                return Promise.reject(error);
            });
        }

        return Promise.resolve({queryPingSupported: false});
    }

    /**
     * @param {String} nid Node id.
     * @param {Number} queryId
     * @param {Number} pageSize
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    queryNextPage(nid, queryId, pageSize) {
        if (this.available(IGNITE_2_0))
            return this.visorTask('queryFetchX2', nid, queryId, pageSize);

        return this.visorTask('queryFetch', nid, queryId, pageSize);
    }

    /**
     * @param {String} nid Node id.
     * @param {Number} [queryId]
     * @returns {Promise<Void>}
     */
    queryClose(nid, queryId) {
        if (this.available(IGNITE_2_0)) {
            return this.visorTask('queryCloseX2', nid, 'java.util.Map', 'java.util.UUID', 'java.util.Collection',
                nid + '=' + queryId);
        }

        return this.visorTask('queryClose', nid, nid, queryId);
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} filter Filter text.
     * @param {Boolean} regEx Flag whether filter by regexp.
     * @param {Boolean} caseSensitive Case sensitive filtration.
     * @param {Boolean} near Scan near cache.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {Number} pageSize Page size.
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    queryScan({nid, cacheName, filter, regEx, caseSensitive, near, local, pageSize}) {
        if (this.available(IGNITE_2_0)) {
            return this.visorTask('queryScanX2', nid, cacheName, filter, regEx, caseSensitive, near, local, pageSize)
                .then(({error, result}) => {
                    if (_.isEmpty(error))
                        return result;

                    return Promise.reject(error);
                });
        }

        /** Prefix for node local key for SCAN near queries. */
        const SCAN_CACHE_WITH_FILTER = 'VISOR_SCAN_CACHE_WITH_FILTER';

        /** Prefix for node local key for SCAN near queries. */
        const SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE = 'VISOR_SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE';

        const prefix = caseSensitive ? SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE : SCAN_CACHE_WITH_FILTER;
        const query = `${prefix}${filter}`;

        return this.querySql({nid, cacheName, query, nonCollocatedJoins: false, enforceJoinOrder: false, replicatedOnly: false, local, pageSize});
    }

    /**
     * Change cluster active state.
     *
     * @returns {Promise}
     */
    toggleClusterState() {
        const { cluster } = this.connectionSbj.getValue();
        const active = !cluster.active;

        return this.visorTask('toggleClusterState', null, active)
            .then(() => this.updateCluster({ ...cluster, active }));
    }

    hasCredentials(clusterId) {
        return this.clustersSecrets.get(clusterId).hasCredentials();
    }
}

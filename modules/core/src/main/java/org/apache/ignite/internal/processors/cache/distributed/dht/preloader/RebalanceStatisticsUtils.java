/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.NotNull;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.nonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Utility class for rebalance statistics.
 */
class RebalanceStatisticsUtils {
    /** To format the date and time. */
    private static final DateTimeFormatter DTF = ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    // TODO: kirill удалить
    /** Text for successful or not rebalances. */
    private static final String SUCCESSFUL_OR_NOT_REBALANCE_TEXT = "including successful and not rebalances";
    /** Text successful rebalance. */
    private static final String SUCCESSFUL_REBALANCE_TEXT = "successful rebalance";

    /** Supplier statistics header. */
    private static final String SUP_STAT_HEAD = "Supplier statistics: ";

    /** Supplier statistics aliases. */
    private static final String SUP_STAT_ALIASES = "Aliases: p - partitions, e - entries, b - bytes, d - duration, " +
        "h - historical, nodeId mapping (nodeId=id,consistentId) ";

    /**
     * Private constructor.
     */
    private RebalanceStatisticsUtils() {
        throw new RuntimeException("don't create");
    }

    /**
     * Returns ability to print statistics for rebalance depending on
     * {@link IgniteSystemProperties#IGNITE_QUIET IGNITE_QUIET} and
     * {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS
     * IGNITE_WRITE_REBALANCE_STATISTICS}.
     * <br/>
     * {@code True} returns only if {@code IGNITE_QUIET = false} and
     * {@code IGNITE_WRITE_REBALANCE_STATISTICS = true}, otherwise
     * {@code false}.
     *
     * @return {@code True} if printing statistics for rebalance is available.
     */
    public static boolean availablePrintRebalanceStatistics() {
        return !getBoolean(IGNITE_QUIET, true) && getBoolean(IGNITE_WRITE_REBALANCE_STATISTICS, false);
    }

    /**
     * Returns ability to print partitions distribution depending on
     * {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS}.
     * <br/>
     * {@code True} returns only if
     * {@code IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS = true}, otherwise
     * {@code false}.
     *
     * @return {@code True} if printing partitions distribution is available.
     */
    public static boolean availablePrintPartitionsDistribution() {
        return getBoolean(IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, false);
    }

    /**
     * Creating a string representation of group cache rebalance statistics for
     * logging.
     *
     * @param cacheGrpCtx Cache group context.
     * @param stat Rebalance statistics.
     * @param suc Flag for successful rebalancing.
     * @param top Topology version.
     * @return String representation of rebalance statistics for cache group.
     */
    public static String cacheGroupRebalanceStatistics(
        CacheGroupContext cacheGrpCtx,
        CacheGroupRebalanceStatistics stat,
        boolean suc,
        AffinityTopologyVersion top
    ) {
        SB sb = new SB();

        sb.a("Information per cache group (").a(suc ? "successful" : "interrupted").a(" rebalance): [")
            .a(grpInfo(cacheGrpCtx)).a(", ").a(time(stat.start(), stat.end())).a(", restarted=")
            .a(stat.attempt() - 1).a("] ");

        Map<ClusterNode, CacheGroupSupplierRebalanceStatistics> supStats = stat.supplierStatistics();
        if (supStats.isEmpty())
            return sb.toString();

        sb.a(SUP_STAT_HEAD);

        int nodeId = 0;
        for (CacheGroupSupplierRebalanceStatistics supStat : supStats.values()) {
            long fp = 0, hp = 0;

            for (Entry<Integer, Boolean> pe : supStat.partitions().entrySet()) {
                if (pe.getValue())
                    fp++;
                else
                    hp++;
            }

            long fe = supStat.fullEntries().sum(), he = supStat.histEntries().sum();
            long fb = supStat.fullBytes().sum(), hb = supStat.histBytes().sum();

            sb.a(supInfo(nodeId++, fp, hp, fe, he, fb, hb, supStat.start(), supStat.end()));
        }

        sb.a(SUP_STAT_ALIASES);

        nodeId = 0;
        for (ClusterNode supNode : supStats.keySet())
            sb.a(supInfo(nodeId++, supNode));

        if (!availablePrintPartitionsDistribution())
            return sb.toString();

        sb.a("Partitions distribution per cache group (").a(suc ? "successful" : "interrupted").a(" rebalance): [")
            .a(grpInfo(cacheGrpCtx)).a("] ");

        SortedMap<Integer, Boolean> parts = new TreeMap<>();
        for (CacheGroupSupplierRebalanceStatistics supStat : supStats.values())
            parts.putAll(supStat.partitions());

        AffinityAssignment aff = cacheGrpCtx.affinity().cachedAffinity(top);

        Map<ClusterNode, Integer> affNodes = new HashMap<>();
        nodeId = 0;
        for (ClusterNode affNode : aff.nodes())
            affNodes.put(affNode, nodeId++);

        SortedSet<ClusterNode> partNodes = new TreeSet<>(comparing(affNodes::get));
        boolean f;

        for (Entry<Integer, Boolean> part : parts.entrySet()) {
            partNodes.clear();

            int p = part.getKey();
            partNodes.addAll(aff.get(p));

            f = true;

            sb.a(p).a(" = ");
            for (ClusterNode partNode : partNodes) {
                if (!f)
                    sb.a(',');
                else
                    f = false;

                sb.a('[').a(affNodes.get(partNode)).a(aff.primaryPartitions(partNode.id()).contains(p) ? ",pr" : ",bu")
                    .a(supStats.containsKey(partNode) ? ",su" : "").a("]");

            }
            sb.a(!part.getValue() ? " h " : " ");
        }

        sb.a("Aliases: pr - primary, bu - backup, su - supplier node, h - historical, nodeId mapping ")
            .a("(nodeId=id,consistentId) ");

        partNodes.addAll(affNodes.keySet());

        for (ClusterNode affNode : partNodes)
            sb.a(supInfo(affNodes.get(affNode), affNode));

        return sb.toString();
    }

    /**
     * Creating a string representation of total rebalance statistics for all
     * cache groups for logging.
     *
     * @param totalStat Statistics of rebalance for cache groups.
     * @return String representation of total rebalance statistics
     *      for all cache groups.
     */
    public static String totalRebalanceStatistic(Map<CacheGroupContext, CacheGroupTotalRebalanceStatistics> totalStat) {
        SB sb = new SB();

        long start = totalStat.values().stream().mapToLong(CacheGroupTotalRebalanceStatistics::start).min().orElse(0);
        long end = totalStat.values().stream().mapToLong(CacheGroupTotalRebalanceStatistics::end).max().orElse(0);

        sb.a("Total information (including successful and not rebalances): [").a(time(start, end)).a("] ");

        Set<ClusterNode> supNodes = new HashSet<>();
        for (CacheGroupTotalRebalanceStatistics stat : totalStat.values())
            supNodes.addAll(stat.supplierStatistics().keySet());

        if (supNodes.isEmpty())
            return sb.toString();

        sb.a(SUP_STAT_HEAD);

        int nodeId = 0;
        for (ClusterNode supNode : supNodes) {
            long fp = 0, hp = 0, fe = 0, he = 0, fb = 0, hb = 0, s = 0, e = 0;

            for (CacheGroupTotalRebalanceStatistics stat : totalStat.values()) {
                if (!stat.supplierStatistics().containsKey(supNode))
                    continue;

                CacheGroupTotalSupplierRebalanceStatistics supStat = stat.supplierStatistics().get(supNode);
                fp += supStat.fullParts().sum();
                hp += supStat.histParts().sum();
                fe += supStat.fullEntries().sum();
                he += supStat.histEntries().sum();
                fb += supStat.fullBytes().sum();
                hb += supStat.histBytes().sum();
                s = s == 0 ? supStat.start().get() : min(supStat.start().get(), s);
                e = max(supStat.end().get(), e);
            }

            sb.a(supInfo(nodeId++, fp, hp, fe, he, fb, hb, s, e));
        }

        sb.a(SUP_STAT_ALIASES);

        nodeId = 0;
        for (ClusterNode supNode : supNodes)
            sb.a(supInfo(nodeId++, supNode));

        return sb.toString();
    }

    /**
     * Creation of information by supplier in format:
     * [{@code nodeId} = Consistent id].
     *
     * @param nodeId       Supplier node id.
     * @param supplierNode Supplier node.
     * @return Supplier info string.
     */
    private static String supInfo(int nodeId, ClusterNode supplierNode) {
        return new SB("[").a(nodeId).a('=').a(supplierNode.consistentId().toString()).a("] ").toString();
    }

    /**
     * Creating a rebalance statistics string for supplier.
     *
     * @param nodeId Supplier node id.
     * @param fp Counter of partitions received by full rebalance.
     * @param hp Counter of partitions received by historical rebalance.
     * @param fe Counter of entries received by full rebalance.
     * @param he Counter of entries received by historical rebalance.
     * @param fb Counter of bytes received by full rebalance.
     * @param hb Counter of bytes received by historical rebalance.
     * @param s Start time of rebalance in milliseconds.
     * @param e End time of rebalance in milliseconds.
     * @return Supplier info string.
     */
    private static String supInfo(int nodeId, long fp, long hp, long fe, long he, long fb, long hb, long s, long e) {
        SB sb = new SB();
        sb.a("[nodeId=").a(nodeId);

        if (fp > 0)
            sb.a(", p=").a(fp);

        if (hp > 0)
            sb.a(", hp=").a(hp);

        if (fe > 0)
            sb.a(", e=").a(fe);

        if (he > 0)
            sb.a(", he=").a(he);

        if (fb > 0)
            sb.a(", b=").a(fb);

        if (hb > 0)
            sb.a(", hb=").a(hb);

        return sb.a(", d=").a(e - s).a(" ms] ").toString();
    }

    /**
     * Creating a string with time information.
     *
     * @param start Start time in milliseconds.
     * @param end   End time in milliseconds.
     * @return String with time information.
     */
    private static String time(long start, long end) {
        return new SB().a("startTime=").a(DTF.format(toLocalDateTime(start))).a(", finishTime=")
            .a(DTF.format(toLocalDateTime(start))).a(", d=").a(end - start).a(" ms").toString();
    }

    /**
     * Creating information for a cache group.
     *
     * @param cacheGrpCtx Cache group context.
     * @return Group info.
     */
    private static String grpInfo(CacheGroupContext cacheGrpCtx) {
        return new SB().a("id=").a(cacheGrpCtx.groupId()).a(", name=").a(cacheGrpCtx.cacheOrGroupName()).toString();
    }

    /**
     * Convert time in millis to local date time.
     *
     * @param time Time in mills.
     * @return The local date-time.
     */
    private static LocalDateTime toLocalDateTime(final long time) {
        return new Date(time).toInstant().atZone(systemDefault()).toLocalDateTime();
    }

    // TODO: kirill удалить все ниже

    /** Rebalance future statistics. */
    static class RebalanceFutureStatistics {
        /** Start rebalance time in mills. */
        private final long startTime = currentTimeMillis();

        /** End rebalance time in mills. */
        private volatile long endTime = startTime;

        /** Per node stats. */
        private final Map<ClusterNode, RebalanceMessageStatistics> msgStats = new ConcurrentHashMap<>();

        /** Is needed or not to print rebalance statistics. */
        private final boolean printRebalanceStatistics = availablePrintRebalanceStatistics();

        /**
         * Add new message statistics.
         * Requires to be invoked before demand message sending.
         * This method required for {@code addReceivePartitionStatistics}.
         * This method add new message statistics if
         * {@link #printRebalanceStatistics} == true.
         *
         * @param supplierNode Supplier node, require not null.
         * @see RebalanceMessageStatistics
         * @see #addReceivePartitionStatistics(ClusterNode, GridDhtPartitionSupplyMessage)
         */
        public void addMessageStatistics(final @NotNull ClusterNode supplierNode) {
            if (!printRebalanceStatistics)
                return;

            msgStats.putIfAbsent(supplierNode, new RebalanceMessageStatistics(currentTimeMillis()));
        }

        /**
         * Add new statistics by receive message with partitions from supplier
         * node. Require invoke {@code addMessageStatistics} before send
         * demand message. This method add new message statistics if
         * {@link #printRebalanceStatistics} == true.
         *
         * @param supplierNode Supplier node, require not null.
         * @param supplyMsg Supply message, require not null.
         * @see ReceivePartitionStatistics
         * @see #addMessageStatistics(ClusterNode)
         */
        public void addReceivePartitionStatistics(
            final ClusterNode supplierNode,
            final GridDhtPartitionSupplyMessage supplyMsg
        ) {
            assert nonNull(supplierNode);
            assert nonNull(supplyMsg);

            if (!printRebalanceStatistics)
                return;

            List<PartitionStatistics> partStats = supplyMsg.infos().entrySet().stream()
                .map(entry -> new PartitionStatistics(entry.getKey(), entry.getValue().infos().size()))
                .collect(toList());

            msgStats.get(supplierNode).receivePartStats
                .add(new ReceivePartitionStatistics(currentTimeMillis(), supplyMsg.messageSize(), partStats));
        }

        /**
         * Clear statistics.
         */
        public void clear() {
            msgStats.clear();
        }

        /**
         * Set end rebalance time in mills.
         *
         * @param endTime End rebalance time in mills.
         */
        public void endTime(final long endTime) {
            this.endTime = endTime;
        }
    }

    /** Rebalance messages statistics. */
    static class RebalanceMessageStatistics {
        /** Time send demand message in mills. */
        private final long sndMsgTime;

        /** Statistics by received partitions. */
        private final Collection<ReceivePartitionStatistics> receivePartStats = new ConcurrentLinkedQueue<>();

        /**
         * Constructor.
         *
         * @param sndMsgTime time send demand message.
         */
        public RebalanceMessageStatistics(final long sndMsgTime) {
            this.sndMsgTime = sndMsgTime;
        }
    }

    /** Receive partition statistics. */
    static class ReceivePartitionStatistics {
        /** Time receive message(on demand message) with partition in mills. */
        private final long rcvMsgTime;

        /** Size receive message in bytes. */
        private final long msgSize;

        /** Received partitions. */
        private final List<PartitionStatistics> parts;

        /**
         * Constructor.
         *
         * @param rcvMsgTime time receive message in mills.
         * @param msgSize message size in bytes.
         * @param parts received partitions, require not null.
         */
        public ReceivePartitionStatistics(
            final long rcvMsgTime,
            final long msgSize,
            final List<PartitionStatistics> parts
        ) {
            assert nonNull(parts);

            this.rcvMsgTime = rcvMsgTime;
            this.msgSize = msgSize;
            this.parts = parts;
        }
    }

    /** Received partition info. */
    static class PartitionStatistics {
        /** Partition id. */
        private final int id;

        /** Count entries in partition. */
        private final int entryCount;

        /**
         * Constructor.
         *
         * @param id partition id.
         * @param entryCount count entries in partitions.
         */
        public PartitionStatistics(final int id, final int entryCount) {
            this.id = id;
            this.entryCount = entryCount;
        }
    }

    /**
     * Return rebalance statistics. Required to call this method if
     * {@link #availablePrintRebalanceStatistics()} == true.
     * <p/>
     * Flag {@code finish} should reflect was full rebalance finished or not.
     * <br/>
     * If {@code finish} == true then expected {@code rebFutrs} contains
     * successful or not {@code RebalanceFuture} per cache group, else expected
     * {@code rebFutrs} contains only one successful {@code RebalanceFuture}.
     * <br/>
     * If {@code finish} == true then print total statistics.
     * <p/>
     * Partition distribution is printed only for last success rebalance,
     * per cache group.
     *
     * @param finish Is the whole rebalance finished or not.
     * @param rebFutrs Involved in rebalance, require not null.
     * @return String with printed rebalance statistics.
     * @throws IgniteCheckedException Could be thrown while getting result of
     *      {@code RebalanceFuture}.
     * @see RebalanceFuture RebalanceFuture
     */
    public static String rebalanceStatistics(
        final boolean finish,
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs
    ) throws IgniteCheckedException {
        assert nonNull(rebFutrs);
        assert availablePrintRebalanceStatistics() : "Can't print statistics";

        AtomicInteger nodeCnt = new AtomicInteger();

        Map<ClusterNode, Integer> nodeAliases = toRebalanceFutureStream(rebFutrs)
            .flatMap(future -> future.stat.msgStats.keySet().stream())
            .distinct()
            .collect(toMap(identity(), node -> nodeCnt.getAndIncrement()));

        StringJoiner joiner = new StringJoiner(" ");

        if (finish)
            writeTotalRebalanceStatistics(rebFutrs, nodeAliases, joiner);

        writeCacheGroupsRebalanceStatistics(rebFutrs, nodeAliases, finish, joiner);
        writeAliasesRebalanceStatistics("p - partitions, e - entries, b - bytes, d - duration", nodeAliases, joiner);
        writePartitionsDistributionRebalanceStatistics(rebFutrs, nodeAliases, nodeCnt, joiner);

        return joiner.toString();
    }

    /**
     * Write total statistics for rebalance.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void writeTotalRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        long minStartTime = minStartTime(toRebalanceFutureStream(rebFutrs));
        long maxEndTime = maxEndTime(toRebalanceFutureStream(rebFutrs));

        joiner.add("Total information (" + SUCCESSFUL_OR_NOT_REBALANCE_TEXT + "):")
            .add("[" + toStartEndDuration(minStartTime, maxEndTime) + "]");

        Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat =
            toSupplierStatistics(toRebalanceFutureStream(rebFutrs));

        writeSupplierRebalanceStatistics(supplierStat, nodeAliases, joiner);
    }

    /**
     * Write rebalance statistics per cache group.
     * <p/>
     * If {@code finish} == true then add {@link #SUCCESSFUL_OR_NOT_REBALANCE_TEXT} else add {@link
     * #SUCCESSFUL_REBALANCE_TEXT} into header.
     *
     * @param rebFuts Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     * @param finish Is finish rebalance.
     */
    private static void writeCacheGroupsRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFuts,
        final Map<ClusterNode, Integer> nodeAliases,
        final boolean finish,
        final StringJoiner joiner
    ) {
        assert nonNull(rebFuts);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        joiner.add("Information per cache group (" +
            (finish ? SUCCESSFUL_OR_NOT_REBALANCE_TEXT : SUCCESSFUL_REBALANCE_TEXT) + "):");

        rebFuts.forEach((context, futures) -> {
            long minStartTime = minStartTime(futures.stream());
            long maxEndTime = maxEndTime(futures.stream());

            joiner.add("[id=" + context.groupId() + ",")
                .add("name=" + context.cacheOrGroupName() + ",")
                .add(toStartEndDuration(minStartTime, maxEndTime) + "]");

            Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat = toSupplierStatistics(futures.stream());
            writeSupplierRebalanceStatistics(supplierStat, nodeAliases, joiner);
        });
    }

    /**
     * Write partitions distribution per cache group. Only for last success rebalance.
     * Works if {@link #availablePrintPartitionsDistribution()} return true.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param nodeCnt For adding new nodes into {@code nodeAliases}, require not null.
     * @param joiner For write statistics, require not null.
     * @throws IgniteCheckedException When get result of
     *      {@link RebalanceFuture}.
     */
    private static void writePartitionsDistributionRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final AtomicInteger nodeCnt,
        final StringJoiner joiner
    ) throws IgniteCheckedException {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(nodeCnt);
        assert nonNull(joiner);

        if (!availablePrintPartitionsDistribution())
            return;

        joiner.add("Partitions distribution per cache group (" + SUCCESSFUL_REBALANCE_TEXT + "):");

        Comparator<RebalanceFuture> startTimeCmp = comparingLong(fut -> fut.stat.startTime);
        Comparator<RebalanceFuture> startTimeCmpReversed = startTimeCmp.reversed();

        Comparator<PartitionStatistics> partIdCmp = comparingInt(value -> value.id);
        Comparator<ClusterNode> nodeAliasesCmp = comparingInt(nodeAliases::get);

        for (Entry<CacheGroupContext, Collection<RebalanceFuture>> rebFutrsEntry : rebFutrs.entrySet()) {
            CacheGroupContext cacheGrpCtx = rebFutrsEntry.getKey();

            joiner.add("[id=" + cacheGrpCtx.groupId() + ",")
                .add("name=" + cacheGrpCtx.cacheOrGroupName() + "]");

            List<RebalanceFuture> successFutures = new ArrayList<>();

            for (RebalanceFuture rebalanceFuture : rebFutrsEntry.getValue()) {
                if (rebalanceFuture.isDone() && rebalanceFuture.get())
                    successFutures.add(rebalanceFuture);
            }

            if (successFutures.isEmpty())
                return;

            successFutures.sort(startTimeCmpReversed);

            RebalanceFuture lastSuccessFuture = successFutures.get(0);

            AffinityAssignment affinity = cacheGrpCtx.affinity().cachedAffinity(lastSuccessFuture.topologyVersion());

            Map<PartitionStatistics, ClusterNode> supplierNodeRcvParts = new TreeMap<>(partIdCmp);

            for (Entry<ClusterNode, RebalanceMessageStatistics> entry : lastSuccessFuture.stat.msgStats.entrySet()) {
                for (ReceivePartitionStatistics receivePartStat : entry.getValue().receivePartStats) {
                    for (PartitionStatistics partStat : receivePartStat.parts)
                        supplierNodeRcvParts.put(partStat, entry.getKey());
                }
            }

            affinity.nodes().forEach(node -> nodeAliases.computeIfAbsent(node, node1 -> nodeCnt.getAndIncrement()));

            for (Entry<PartitionStatistics, ClusterNode> supplierNodeRcvPart : supplierNodeRcvParts.entrySet()) {
                int partId = supplierNodeRcvPart.getKey().id;

                String nodes = affinity.get(partId).stream()
                    .sorted(nodeAliasesCmp)
                    .map(node -> "[" + nodeAliases.get(node) +
                        (affinity.primaryPartitions(node.id()).contains(partId) ? ",pr" : ",bu") +
                        (node.equals(supplierNodeRcvPart.getValue()) ? ",su" : "") + "]"
                    )
                    .collect(joining(","));

                joiner.add(valueOf(partId)).add("=").add(nodes);
            }
        }

        writeAliasesRebalanceStatistics("pr - primary, bu - backup, su - supplier node", nodeAliases, joiner);
    }

    /**
     * Write stattistics per supplier node.
     *
     * @param supplierStat Statistics by supplier (in successful and not rebalances), require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void writeSupplierRebalanceStatistics(
        final Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(supplierStat);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        joiner.add("Supplier statistics:");

        supplierStat.forEach((supplierNode, msgStats) -> {
            long partCnt = sum(msgStats, rps -> rps.parts.size());
            long byteSum = sum(msgStats, rps -> rps.msgSize);
            long entryCount = sum(msgStats, rps -> rps.parts.stream().mapToLong(ps -> ps.entryCount).sum());

            long durationSum = msgStats.stream()
                .flatMapToLong(msgStat -> msgStat.receivePartStats.stream()
                    .mapToLong(rps -> rps.rcvMsgTime - msgStat.sndMsgTime)
                )
                .sum();

            joiner.add("[nodeId=" + nodeAliases.get(supplierNode) + ",")
                .add(toPartitionsEntriesBytes(partCnt, entryCount, byteSum) + ",")
                .add("d=" + durationSum + " ms]");
        });
    }

    /**
     * Write statistics aliases, for reducing output string.
     *
     * @param nodeAliases for print nodeId=1 instead long string, require not null.
     * @param abbreviations Abbreviations ex. b - bytes, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void writeAliasesRebalanceStatistics(
        final String abbreviations,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(abbreviations);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        String nodes = nodeAliases.entrySet().stream()
            .sorted(comparingInt(Entry::getValue))
            .map(entry -> "[" + entry.getValue() + "=" + entry.getKey().id() + "," + entry.getKey().consistentId() + "]")
            .collect(joining(", "));

        joiner.add("Aliases:").add(abbreviations + ",").add("nodeId mapping (nodeId=id,consistentId)").add(nodes);
    }

    /**
     * Get min {@link RebalanceFutureStatistics#startTime} in stream rebalance future's.
     *
     * @param stream Stream rebalance future, require not null.
     * @return Min start time.
     */
    private static long minStartTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.startTime).min().orElse(0);
    }

    /**
     * Get max {@link RebalanceFutureStatistics#endTime} in stream rebalance future's.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Max end time.
     */
    private static long maxEndTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.endTime).max().orElse(0);
    }

    /**
     * Prepare stream rebalance future's of each cache groups.
     *
     * @param rebFutrs Rebalance future's by cache groups, require not null.
     * @return Stream rebalance future's.
     */
    private static Stream<RebalanceFuture> toRebalanceFutureStream(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs
    ) {
        assert nonNull(rebFutrs);

        return rebFutrs.entrySet().stream().flatMap(entry -> entry.getValue().stream());
    }

    /**
     * Aggregates statistics by supplier node.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Statistic by supplier.
     */
    private static Map<ClusterNode, List<RebalanceMessageStatistics>> toSupplierStatistics(
        final Stream<RebalanceFuture> stream
    ) {
        assert nonNull(stream);

        return stream.flatMap(future -> future.stat.msgStats.entrySet().stream())
            .collect(groupingBy(Entry::getKey, mapping(Entry::getValue, toList())));
    }

    /**
     * Creates a string containing the beginning, end, and duration of the rebalance.
     *
     * @param start Start time in ms.
     * @param end End time in ms.
     * @return Formatted string of rebalance time.
     * @see #DTF
     */
    private static String toStartEndDuration(final long start, final long end) {
        return "startTime=" + DTF.format(toLocalDateTime(start)) + ", finishTime=" +
            DTF.format(toLocalDateTime(end)) + ", d=" + (end - start) + " ms";
    }

    /**
     * Summarizes long values.
     *
     * @param msgStats Message statistics, require not null.
     * @param longExtractor Long extractor, require not null.
     * @return Sum of long values.
     */
    private static long sum(
        final List<RebalanceMessageStatistics> msgStats,
        final ToLongFunction<? super ReceivePartitionStatistics> longExtractor
    ) {
        assert nonNull(msgStats);
        assert nonNull(longExtractor);

        return msgStats.stream()
            .flatMap(msgStat -> msgStat.receivePartStats.stream())
            .mapToLong(longExtractor)
            .sum();
    }

    /**
     * Create a string containing count received partitions,
     * count received entries and sum received bytes.
     *
     * @param parts Count received partitions.
     * @param entries Count received entries.
     * @param bytes Sum received bytes.
     * @return Formatted string of received rebalance partitions.
     */
    private static String toPartitionsEntriesBytes(final long parts, final long entries, final long bytes) {
        return "p=" + parts + ", e=" + entries + ", b=" + bytes;
    }
}

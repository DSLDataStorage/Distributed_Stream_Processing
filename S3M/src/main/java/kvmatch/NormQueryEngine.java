/*
 * Copyright 2018 Jiaye Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kvmatch;

import kvmatch.common.*;
import kvmatch.common.entity.IndexNode;
import kvmatch.operator.IndexOperator;
import kvmatch.operator.TimeSeriesOperator;
import kvmatch.operator.file.IndexFileOperator;
import kvmatch.operator.file.TimeSeriesFileOperator;
import kvmatch.utils.MeanIntervalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Query engine for KV-index_{DP} with normalization under ED
 * <p>
 * Created by Jiaye Wu on 18-1-10.
 */
@SuppressWarnings("Duplicates")
public class NormQueryEngine {

    private static final Logger logger = LoggerFactory.getLogger(NormQueryEngine.class.getName());

    // \Sigma = {25, 50, 100, 200, 400}
    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true};

    private static final boolean ENABLE_EARLY_TERMINATION = true;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_A = 9.72276547123376;
    private static final double PHASE_2_TIME_ESTIMATE_COEFFICIENT_B = 0.0106737255022236;
    private static final double PHASE_2_TIME_ESTIMATE_INTERCEPT = 0.0;
    private static final boolean ENABLE_QUERY_REORDERING = true;
    private static final boolean ENABLE_INCREMENTAL_VISITING = true;
    private static final boolean ENABLE_STD_FILTER = true;
    private static final boolean ENABLE_BETA_PARTITION = true;
    private static final double BETA_PARTITION_WIDTH = 10.0;
    private static final int MAX_SCAN_DATA_LENGTH = 40000;

    private TimeSeriesOperator timeSeriesOperator = null;
    private IndexOperator[] indexOperators = new IndexOperator[WuList.length];
    private List<List<Pair<Double, Pair<Integer, Integer>>>> statisticInfos = new ArrayList<>(WuList.length);
    private List<List<IndexCache>> indexCaches = new ArrayList<>(WuList.length);
    private int n, cntScans;
    private double[] prefixSums;
    private double[][] cost;
    private int[][] cost2;

    public NormQueryEngine(int n, String storageType) throws IOException {
        this.n = n;
        timeSeriesOperator = new TimeSeriesFileOperator(n, false);
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;
            indexOperators[i] = new IndexFileOperator("standard", n, WuList[i], false);
        }
        loadMetaTable();
    }

    public List<Pair<Integer, Double>> query(List<Double> queryData, double epsilon, double alpha, double beta) throws IOException {
        // initialization: clear cache
        if (ENABLE_INCREMENTAL_VISITING) {
            indexCaches.clear();
            for (int ignored : WuList) {
                indexCaches.add(new ArrayList<>());
            }
        }
        cntScans = 0;
        int queryLength = queryData.size();

        long startTime = System.currentTimeMillis();

        // Phase 0: calculate statistics for the query series
        // calculate mean and std of whole query series
        double ex = 0, ex2 = 0;
        for (Double value : queryData) {
            ex += value;
            ex2 += value * value;
        }
        double meanQ = ex / queryLength;
        double stdQ = Math.sqrt(ex2 / queryLength - meanQ * meanQ);
        logger.info("meanQ: {}, stdQ: {}", meanQ, stdQ);
        // dynamic programming
        List<QuerySegment> queries = determineQueryPlan(queryData, epsilon, alpha, beta, meanQ, stdQ);
        logger.info("Query order: {}", queries);

        // Phase 1: index-probing
        long startTime1 = System.currentTimeMillis();

        List<NormInterval> validPositions = new ArrayList<>();  // CS

        int lastSegment = queries.get(queries.size() - 1).getOrder();
        double lastTotalTimeUsageEstimated = Double.MAX_VALUE;
        int preLength = 0;
        for (int i = 0; i < queries.size(); i++) {
            QuerySegment query = queries.get(i);
            logger.info("Disjoint window #{} - {} - mean: {}", i + 1, query.getOrder(), query.getMean());

            int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder()) * WuList[0];
            preLength += query.getWu() / WuList[0];

            List<NormInterval> nextValidPositions = new ArrayList<>();  // CS

            // store possible current segment
            List<NormInterval> positions = new ArrayList<>();  // CS_i

            // query possible rows which mean is in distance range of i-th disjoint window
            double beginRound = 1.0 / alpha * query.getMean() + (1 - 1.0 / alpha) * meanQ - beta - Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
            double beginRound1 = alpha * query.getMean() + (1 - alpha) * meanQ - beta - Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
            beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1), statisticInfos.get(query.getWu() / WuList[0] - 1));

            double endRound = alpha * query.getMean() + (1 - alpha) * meanQ + beta + Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
            double endRound1 = 1.0 / alpha * query.getMean() + (1 - 1.0 / alpha) * meanQ + beta + Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
            endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));

            // beta partitions
            int betaPartitionNum = 1;
            if (ENABLE_BETA_PARTITION) {
                betaPartitionNum = (int) (2.0 * beta / BETA_PARTITION_WIDTH);
                if (betaPartitionNum > 64) {  // at most 64 bits
                    betaPartitionNum = 64;
                }
            }
            List<Pair<Double, Double>> betaPartitions = new ArrayList<>(betaPartitionNum);
            for (int betaPartitionIdx = 0; betaPartitionIdx < betaPartitionNum; betaPartitionIdx++) {
                double betaPartition = 2.0 * beta / betaPartitionNum;

                double beginRoundT = 1.0 / alpha * query.getMean() + (1 - 1.0 / alpha) * meanQ - beta + betaPartition * betaPartitionIdx - Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
                double beginRound1T = alpha * query.getMean() + (1 - alpha) * meanQ - beta + betaPartition * betaPartitionIdx - Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
                beginRoundT = MeanIntervalUtils.toRound(Math.min(beginRoundT, beginRound1T), statisticInfos.get(query.getWu() / WuList[0] - 1));

                double endRoundT = alpha * query.getMean() + (1 - alpha) * meanQ - beta + betaPartition * (betaPartitionIdx + 1) + Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
                double endRound1T = 1.0 / alpha * query.getMean() + (1 - 1.0 / alpha) * meanQ - beta + betaPartition * (betaPartitionIdx + 1) + Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
                endRoundT = MeanIntervalUtils.toRound(Math.max(endRoundT, endRound1T));

                betaPartitions.add(new Pair<>(beginRoundT, endRoundT));
//                logger.info("Beta partition #{}: {} - {}", betaPartitionIdx + 1, beginRoundT, endRoundT);
            }

            logger.info("Scan index from {} to {}", beginRound, endRound);
            if (ENABLE_INCREMENTAL_VISITING) {
                int cacheIndex = query.getWu() / WuList[0] - 1;
                int index_l = findCache(cacheIndex, beginRound);
                int index_r = findCache(cacheIndex, endRound, index_l);

                if (index_l == index_r && index_l >= 0) {
                    /*
                     * Current:          l|===|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_____|index_l_r
                     */
                    scanCache(index_l, beginRound, true, endRound, true, query, positions, betaPartitions);
                } else if (index_l < 0 && index_r >= 0) {
                    /*
                     * Current:         l|_==|r
                     * Cache  :   index_r_l|_____|index_r_r
                     * Future : index_r_l|_______|index_r_r
                     */
                    scanCache(index_r, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), true, endRound, true, query, positions, betaPartitions);
                    scanIndexAndAddCache(beginRound, true, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), false, index_r, query, positions, betaPartitions);
                    indexCaches.get(cacheIndex).get(index_r).setBeginRound(beginRound);
                } else if (index_l >= 0 && index_r < 0) {
                    /*
                     * Current:             l|==_|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_______|index_l_r
                     */
                    scanCache(index_l, beginRound, true, indexCaches.get(cacheIndex).get(index_l).getEndRound(), true, query, positions, betaPartitions);
                    scanIndexAndAddCache(indexCaches.get(cacheIndex).get(index_l).getEndRound(), false, endRound, true, index_l, query, positions, betaPartitions);
                    indexCaches.get(cacheIndex).get(index_l).setEndRound(endRound);
                } else if (index_l == index_r && index_l < 0) {
                    /*
                     * Current:        l|___|r
                     * Cache  : |_____|       |_____|
                     * Future : |_____|l|___|r|_____|
                     */
                    scanIndexAndAddCache(beginRound, true, endRound, true, index_r, query, positions, betaPartitions);  // insert a new cache node
                } else if (index_l >= 0 && index_r >= 0 && index_l + 1 == index_r) {
                    /*
                      Current:     l|=___=|r
                      Cache  : |_____|s  |_____|
                      Future : |_______________|
                     */
                    double s = indexCaches.get(cacheIndex).get(index_l).getEndRound();
                    scanCache(index_l, beginRound, true, s, true, query, positions, betaPartitions);
                    scanIndexAndAddCache(s, false, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), false, index_r, query, positions, betaPartitions);
                    scanCache(index_r, indexCaches.get(cacheIndex).get(index_r).getBeginRound(), true, endRound, true, query, positions, betaPartitions);
                    indexCaches.get(cacheIndex).get(index_r).setBeginRound(s + 0.01);
                }
            } else {
                scanIndex(beginRound, true, endRound, true, query, positions, betaPartitions);
            }
            positions = sortButNotMergeIntervals(positions);
//            logger.info("position: {}", positions.toString());

            if (i == 0) {
                for (NormInterval position : positions) {
                    if (position.getRight() - (query.getOrder() - 1) * WuList[0] + queryLength - 1 > n) {
                        if (position.getLeft() - (query.getOrder() - 1) * WuList[0] + queryLength - 1 <= n) {
                            nextValidPositions.add(new NormInterval(position.getLeft() + deltaW,
                                    n - queryLength + 1 + (query.getOrder() - 1) * WuList[0] + deltaW,
                                    position.getExLower(), position.getEx2Lower(), position.getBetaPartitions()));
                        }
                    } else if (position.getLeft() - (query.getOrder() - 1) * WuList[0] < 1) {
                        if (position.getRight() - (query.getOrder() - 1) * WuList[0] >= 1) {
                            nextValidPositions.add(new NormInterval(1 + (query.getOrder() - 1) * WuList[0] + deltaW,
                                    position.getRight() + deltaW,
                                    position.getExLower(), position.getEx2Lower(), position.getBetaPartitions()));
                        }
                    } else {
                        nextValidPositions.add(new NormInterval(position.getLeft() + deltaW,
                                position.getRight() + deltaW,
                                position.getExLower(), position.getEx2Lower(), position.getBetaPartitions()));
                    }
                }
            } else {
                int index1 = 0, index2 = 0;  // 1 - CS, 2 - CS_i
                while (index1 < validPositions.size() && index2 < positions.size()) {
                    if (validPositions.get(index1).getRight() < positions.get(index2).getLeft()) {
                        index1++;
                    } else if (positions.get(index2).getRight() < validPositions.get(index1).getLeft()) {
                        index2++;
                    } else {
                        long commonBetaPartitions = 0;
                        if (ENABLE_BETA_PARTITION) {
                            commonBetaPartitions = validPositions.get(index1).getBetaPartitions() & positions.get(index2).getBetaPartitions();
                            if (commonBetaPartitions == 0) {  // no common, abandon the former one
                                if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                    index1++;
                                } else {
                                    index2++;
                                }
                                continue;
                            }
                        }

                        if (ENABLE_STD_FILTER) {
                            double sumEx = validPositions.get(index1).getExLower() + positions.get(index2).getExLower();
                            double sumEx2 = validPositions.get(index1).getEx2Lower() + positions.get(index2).getEx2Lower();
                            double mean = sumEx / preLength;

                            double std2 = 0;
                            if (mean > meanQ + beta) {
                                double newValue = meanQ + beta - (mean - meanQ - beta) * preLength * WuList[0] / (queryLength - preLength * 1.0 * WuList[0]);
                                mean = meanQ + beta;
                                std2 = (sumEx2 * WuList[0] + (queryLength - preLength * WuList[0]) * newValue * newValue) / queryLength - mean * mean;
                            }

                            if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new NormInterval(
                                            Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW,
                                            validPositions.get(index1).getRight() + deltaW,
                                            sumEx, sumEx2, commonBetaPartitions));
                                }
                                index1++;
                            } else {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new NormInterval(
                                            Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW,
                                            positions.get(index2).getRight() + deltaW,
                                            sumEx, sumEx2, commonBetaPartitions));
                                }
                                index2++;
                            }
                        } else {
                            if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                nextValidPositions.add(new NormInterval(
                                        Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW,
                                        validPositions.get(index1).getRight() + deltaW,
                                        0, 0, commonBetaPartitions));
                                index1++;
                            } else {
                                nextValidPositions.add(new NormInterval(
                                        Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW,
                                        positions.get(index2).getRight() + deltaW,
                                        0, 0, commonBetaPartitions));
                                index2++;
                            }
                        }
                    }
                }
            }

            Pair<List<NormInterval>, Pair<Integer, Integer>> candidates = sortButNotMergeIntervalsAndCount(nextValidPositions);
            validPositions = candidates.getFirst();
//            logger.info("next valid: {}", validPositions.toString());

            int cntCurrentDisjointCandidateWindows = candidates.getSecond().getFirst();
            int cntCurrentCandidateOffsets = candidates.getSecond().getSecond();
            logger.info("Disjoint candidate windows: {}, candidate offsets: {}", cntCurrentDisjointCandidateWindows, cntCurrentCandidateOffsets);

            if (ENABLE_EARLY_TERMINATION) {
                int phase1TimeUsageUntilNow = (int) (System.currentTimeMillis() - startTime1);
                double phase2TimeUsageEstimated = PHASE_2_TIME_ESTIMATE_COEFFICIENT_A * cntCurrentDisjointCandidateWindows + PHASE_2_TIME_ESTIMATE_COEFFICIENT_B * cntCurrentCandidateOffsets / 100000 * queryLength + PHASE_2_TIME_ESTIMATE_INTERCEPT;
                double totalTimeUsageEstimated = phase1TimeUsageUntilNow + phase2TimeUsageEstimated;
                logger.info("Time usage: phase 1 until now: {}, phase 2 estimated: {}, total estimated: {}", phase1TimeUsageUntilNow, phase2TimeUsageEstimated, totalTimeUsageEstimated);

                if (i >= 5 && totalTimeUsageEstimated > lastTotalTimeUsageEstimated) {
                    lastSegment = (i == queries.size() - 1) ? query.getOrder() : queries.get(i + 1).getOrder();
                    break;
                }
                lastTotalTimeUsageEstimated = totalTimeUsageEstimated;
            }
        }
        if (!ENABLE_EARLY_TERMINATION) {
            lastSegment = queries.get(queries.size() - 1).getOrder();
        }

        // merge consecutive intervals to shrink data size and alleviate scan times
        validPositions = sortAndMergeIntervals(validPositions);

        long endTime1 = System.currentTimeMillis();

        // Phase 2: post-processing
        long startTime2 = System.currentTimeMillis();

        List<Pair<Integer, Double>> answers = new ArrayList<>();
        int cntCandidate = 0;
        // do z-normalization on query data
        double[] zQ = new double[queryLength];
        for (int i = 0; i < queryLength; i++) {
            zQ[i] = (queryData.get(i) - meanQ) / stdQ;
        }
        // sort the query data
        int[] order = new int[queryLength];
        Index[] Q_tmp = new Index[queryLength];
        for (int i = 0; i < queryLength; i++) {
            Q_tmp[i] = new Index(zQ[i], i);
        }
        Arrays.sort(Q_tmp, (o1, o2) -> Double.compare(Math.abs(o2.value), Math.abs(o1.value)));
        for (int i = 0; i < queryLength; i++) {
            zQ[i] = Q_tmp[i].value;
            order[i] = Q_tmp[i].index;
        }

        int idx = 0;
        while (idx < validPositions.size()) {
            int beginIdx = idx, endIdx = idx;

            int begin = validPositions.get(idx).getLeft() - (lastSegment - 1) * WuList[0];
            int end = validPositions.get(idx).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1;
            if (begin < 1) begin = 1;
            int length = end - begin + 1;
            idx++;

            while (idx < validPositions.size()) {
                begin = validPositions.get(idx).getLeft() - (lastSegment - 1) * WuList[0];
                int newLength = length + begin - end - 1;
                end = validPositions.get(idx).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1;
                if (end > n) end = n;
                newLength += end - begin + 1;
                if (newLength > MAX_SCAN_DATA_LENGTH) break;
                endIdx = idx;
                length = newLength;
                idx++;
            }

            begin = validPositions.get(beginIdx).getLeft() - (lastSegment - 1) * WuList[0];
            end = validPositions.get(endIdx).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1;
            if (begin < 1) begin = 1;
            if (end > n) end = n;
            logger.debug("Scan data [{}, {}]", begin, end);
            @SuppressWarnings("unchecked")
            List<Double> data = timeSeriesOperator.readTimeSeries(begin, end - begin + 1);

            for (int idx1 = beginIdx; idx1 <= endIdx; idx1++) {
                cntCandidate += validPositions.get(idx1).getRight() - validPositions.get(idx1).getLeft() + 1;

                ex = ex2 = 0;
                double[] T = new double[2 * queryLength];

                int begin1 = validPositions.get(idx1).getLeft() - (lastSegment - 1) * WuList[0] - begin;
                int end1 = validPositions.get(idx1).getRight() - (lastSegment - 1) * WuList[0] + queryLength - 1 - begin;

                if (begin1 < 0) begin1 = 0;
                if (end1 > data.size() - 1) end1 = data.size() - 1;
//                logger.info("{} - {}", begin1 + begin, end1 + begin);
                for (int i = begin1; i <= end1; i++) {
                    double d = data.get(i);
                    ex += d;
                    ex2 += d * d;
                    T[i % queryLength] = d;
                    T[(i % queryLength) + queryLength] = d;

                    if (i - begin1 >= queryLength - 1) {
                        // the current starting location of T
                        int j = (i + 1) % queryLength;

                        // z-normalization of T will be calculated on the fly
                        double mean = ex / queryLength;
                        double std = Math.sqrt(ex2 / queryLength - mean * mean);

                        if (Math.abs(mean - meanQ) <= beta && (std / stdQ) <= alpha && (std / stdQ) >= 1.0 / alpha) {
                            // calculate ED distance & test single point range criterion
                            double dist = 0;
                            for (int k = 0; k < queryLength && dist <= epsilon * epsilon; k++) {
                                double x = (T[(order[k] + j)] - mean) / std;
                                dist += (x - zQ[k]) * (x - zQ[k]);
                            }
                            if (dist <= epsilon * epsilon) {
                                answers.add(new Pair<>(begin + i - queryLength + 1, Math.sqrt(dist)));
                            }
                        }

                        ex -= T[j];
                        ex2 -= T[j] * T[j];
                    }
                }
            }
        }

        long endTime2 = System.currentTimeMillis();

        answers.sort(Comparator.comparing(Pair::getSecond));

        if (!answers.isEmpty()) {
            logger.info("Best: {}, distance: {}", answers.get(0).getFirst(), answers.get(0).getSecond());
            System.out.printf("Best: %d, distance: %f\n", answers.get(0).getFirst(), answers.get(0).getSecond());
        }
        logger.info("T: {} ms, T_1: {} ms, T_2: {} ms, #candidates: {}, #answers: {}", endTime2 - startTime, endTime1 - startTime1, endTime2 - startTime2, cntCandidate, answers.size());
        return answers;
    }

    private Pair<Integer, Integer> getCountsFromStatisticInfo(int Wu, double mean, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = statisticInfos.get(Wu / WuList[0] - 1);

        double beginRound = 1.0 / alpha * mean + (1 - 1.0 / alpha) * meanQ - beta - Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / Wu);
        double beginRound1 = alpha * mean + (1 - alpha) * meanQ - beta - Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / Wu);
        beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1));

        double endRound = alpha * mean + (1 - alpha) * meanQ + beta + Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / Wu);
        double endRound1 = 1.0 / alpha * mean + (1 - 1.0 / alpha) * meanQ + beta + Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / Wu);
        endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));

        int index = Collections.binarySearch(statisticInfo, new Pair<>(beginRound, 0), Comparator.comparing(Pair::getFirst));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        int lower1 = index > 0 ? statisticInfo.get(index - 1).getSecond().getFirst() : 0;
        int lower2 = index > 0 ? statisticInfo.get(index - 1).getSecond().getSecond() : 0;

        index = Collections.binarySearch(statisticInfo, new Pair<>(endRound, 0), Comparator.comparing(Pair::getFirst));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        int upper1 = index > 0 ? statisticInfo.get(index).getSecond().getFirst() : 0;
        int upper2 = index > 0 ? statisticInfo.get(index).getSecond().getSecond() : 0;

        return new Pair<>(upper1 - lower1, upper2 - lower2);
    }

    private double getCost(int l, int r, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        if (cost[l][r] != -1) return cost[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double mean = (prefixSums[r] - (l > 0 ? prefixSums[l - 1] : 0)) / useWu;
        Pair<Integer, Integer> counts = getCountsFromStatisticInfo(useWu, mean, epsilon, alpha, beta, meanQ, stdQ);
        cost[l][r] = 1.0 * counts.getFirst() / statisticInfos.get(100 / 25 - 1).get(statisticInfos.get(100 / 25 - 1).size() - 1).getSecond().getFirst();
        cost[l][r] = Math.log(cost[l][r]);
        cost2[l][r] = counts.getFirst();
        return cost[l][r];
    }

    private int getCost2(int l, int r, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        if (cost2[l][r] != -1) return cost2[l][r];
        int useWu = WuList[0] * (r - l + 1);
        double mean = (prefixSums[r] - (l > 0 ? prefixSums[l - 1] : 0)) / useWu;
        Pair<Integer, Integer> counts = getCountsFromStatisticInfo(useWu, mean, epsilon, alpha, beta, meanQ, stdQ);
        cost2[l][r] = counts.getFirst();
        return cost2[l][r];
    }

    private List<QuerySegment> determineQueryPlan(List<Double> queryData, double epsilon, double alpha, double beta, double meanQ, double stdQ) {
        int m = queryData.size() / WuList[0];

        // calculate mean and sum of each disjoint window
        List<Double> sums = new ArrayList<>(m);
        double ex = 0;
        for (int i = 0; i < queryData.size(); i++) {
            ex += queryData.get(i);
            if ((i + 1) % WuList[0] == 0) {
                sums.add(ex);
                ex = 0;
            }
        }

        // initialize for dynamic programming algorithm
        double[][] dp = new double[m + 1][];
        int[][] pre = new int[m + 1][];
        cost = new double[m][];
        cost2 = new int[m][];
        prefixSums = new double[m];
        prefixSums[0] = sums.get(0);
        for (int i = 0; i <= m; i++) {
            dp[i] = new double[m + 1];
            Arrays.fill(dp[i], Double.MAX_VALUE);
            pre[i] = new int[m + 1];
            Arrays.fill(pre[i], -1);
        }
        for (int i = 0; i < m; i++) {
            cost[i] = new double[m];
            Arrays.fill(cost[i], -1);
            cost2[i] = new int[m];
            Arrays.fill(cost2[i], -1);
            if (i > 0) {
                prefixSums[i] = prefixSums[i - 1] + sums.get(i);
            }
        }

        dp[0][0] = 0;
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= Math.min(i, 30); j++) {
                for (int k = 1; k <= WuList.length && i - k >= 0; k++) {
                    if (!WuEnabled[k - 1]) continue;
                    //double tmp = Math.pow(Math.pow(dp[i-k][j-1], j-1) * getCost(i-k, i-1), 1.0 / j);
                    double tmp = ((j - 1) * (dp[i - k][j - 1]) + getCost(i - k, i - 1, epsilon, alpha, beta, meanQ, stdQ)) / j;
                    if (tmp < dp[i][j]) {
                        dp[i][j] = tmp;
                        pre[i][j] = k;
                    }
                }
            }
        }

        // find out the optimal division strategy
        List<QuerySegment> queries = new ArrayList<>();
        double min = Double.MAX_VALUE;
        int index = m, p = -1;
        for (int i = (31 - Integer.numberOfLeadingZeros(queryData.size()) - 1) / 2; i <= Math.min(m, 30); i++) {
            if (dp[m][i] <= min) {
                min = dp[m][i];
                p = i;
            }
        }
        for (int i = p; i >= 0; i--) {
            int l = index - pre[index][i], r = index - 1;
            int useWu = WuList[0] * (r - l + 1);
            if (useWu < 0) break;
            double mean = (prefixSums[r] - (l > 0 ? prefixSums[l - 1] : 0)) / useWu;
            queries.add(new QuerySegment(mean, l + 1, getCost2(l, r, epsilon, alpha, beta, meanQ, stdQ), useWu));
            index -= pre[index][i];
        }

        if (ENABLE_QUERY_REORDERING) {
            // optimize query order
            queries.sort(Comparator.comparingInt(QuerySegment::getCount));
        }

        return queries;
    }

    @SuppressWarnings("SameParameterValue")
    private void scanIndex(double begin, boolean beginInclusive, double end, boolean endInclusive,
                           QuerySegment query, List<NormInterval> positions,
                           List<Pair<Double, Double>> betaPartitions) throws IOException {
        int useWu = query.getWu() / WuList[0];

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperators[useWu - 1].readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfos.get(useWu - 1)) : meanRound;
            long partitions = 0;
            if (ENABLE_BETA_PARTITION) {
                for (int betaPartitionIdx = 0; betaPartitionIdx < betaPartitions.size(); betaPartitionIdx++) {
                    Pair<Double, Double> betaPartition = betaPartitions.get(betaPartitionIdx);
                    if (betaPartition.getFirst() > meanRound) break;
                    if (betaPartition.getFirst() <= meanRound && betaPartition.getSecond() >= meanRound) {
                        partitions |= 1 << betaPartitionIdx;
                    }
                }
            }
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(),
                        meanRound * useWu, meanRound2 * meanRound2 * useWu, partitions));
            }
        }
    }

    private void scanIndexAndAddCache(double begin, boolean beginInclusive, double end, boolean endInclusive,
                                      int index, QuerySegment query, List<NormInterval> positions,
                                      List<Pair<Double, Double>> betaPartitions) throws IOException {
        int useWu = query.getWu() / WuList[0];

        if (index < 0) {
            index = -index - 1;
            indexCaches.get(useWu - 1).add(index, new IndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        Map<Double, IndexNode> indexes = indexOperators[useWu - 1].readIndexes(begin, end);
        cntScans++;
        for (Map.Entry<Double, IndexNode> entry : indexes.entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfos.get(useWu - 1)) : meanRound;
            long partitions = 0;
            if (ENABLE_BETA_PARTITION) {
                for (int betaPartitionIdx = 0; betaPartitionIdx < betaPartitions.size(); betaPartitionIdx++) {
                    Pair<Double, Double> betaPartition = betaPartitions.get(betaPartitionIdx);
                    if (betaPartition.getFirst() > meanRound) break;
                    if (betaPartition.getFirst() <= meanRound && betaPartition.getSecond() >= meanRound) {
                        partitions |= 1 << betaPartitionIdx;
                    }
                }
            }
            for (Pair<Integer, Integer> position : entry.getValue().getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(),
                        meanRound * useWu, meanRound2 * meanRound2 * useWu, partitions));
            }

            indexCaches.get(useWu - 1).get(index).addCache(meanRound, entry.getValue());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void scanCache(int index, double begin, boolean beginInclusive, double end, boolean endInclusive,
                           QuerySegment query, List<NormInterval> positions, List<Pair<Double, Double>> betaPartitions) {
        int useWu = query.getWu() / WuList[0];

        for (Map.Entry<Double, IndexNode> entry : indexCaches.get(useWu - 1).get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfos.get(useWu - 1)) : meanRound;
            long partitions = 0;
            if (ENABLE_BETA_PARTITION) {
                for (int betaPartitionIdx = 0; betaPartitionIdx < betaPartitions.size(); betaPartitionIdx++) {
                    Pair<Double, Double> betaPartition = betaPartitions.get(betaPartitionIdx);
                    if (betaPartition.getFirst() > meanRound) break;
                    if (betaPartition.getFirst() <= meanRound && betaPartition.getSecond() >= meanRound) {
                        partitions |= 1 << betaPartitionIdx;
                    }
                }
            }
            IndexNode indexNode = entry.getValue();
            for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                positions.add(new NormInterval(position.getFirst(), position.getSecond(),
                        meanRound * useWu, meanRound2 * meanRound2 * useWu, partitions));
            }
        }
    }

    private int findCache(int index, double round) {
        return findCache(index, round, 0);
    }

    private int findCache(int index, double round, int first) {
        if (first < 0) {
            first = -first - 1;
        }

        for (int i = first; i < indexCaches.get(index).size(); i++) {
            IndexCache cache = indexCaches.get(index).get(i);
            if (cache.getBeginRound() > round) {
                return -i - 1;
            }
            if (cache.getBeginRound() <= round && cache.getEndRound() >= round) {
                return i;
            }
        }

        return -1;
    }

    private List<NormInterval> sortButNotMergeIntervals(List<NormInterval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingInt(NormInterval::getLeft));

        NormInterval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double ex = first.getExLower();
        double ex2 = first.getEx2Lower();
        long betaPartitions = first.getBetaPartitions();

        List<NormInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);
            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getExLower(), ex) == 0 && Double.compare(current.getEx2Lower(), ex2) == 0)) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getExLower(), ex);
                ex2 = Math.min(current.getEx2Lower(), ex2);
                betaPartitions = current.getBetaPartitions() | betaPartitions;
            } else {
                result.add(new NormInterval(start, end, ex, ex2, betaPartitions));
                start = current.getLeft();
                end = current.getRight();
                ex = current.getExLower();
                ex2 = current.getEx2Lower();
                betaPartitions = current.getBetaPartitions();
            }
        }
        result.add(new NormInterval(start, end, ex, ex2, betaPartitions));

        return result;
    }

    private Pair<List<NormInterval>, Pair<Integer, Integer>> sortButNotMergeIntervalsAndCount(List<NormInterval> intervals) {
        if (intervals.size() <= 1) {
            return new Pair<>(intervals, new Pair<>(intervals.size(), intervals.isEmpty() ? 0 : (intervals.get(0).getRight() - intervals.get(0).getLeft() + 1)));
        }

        intervals.sort(Comparator.comparingInt(NormInterval::getLeft));

        NormInterval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();
        double ex = first.getExLower();
        double ex2 = first.getEx2Lower();
        long betaPartitions = first.getBetaPartitions();

        List<NormInterval> result = new ArrayList<>();

        int cntDisjointIntervals = intervals.size();
        int cntOffsets = 0;
        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);

            if (current.getLeft() - 1 <= end) {  // count for disjoint intervals to estimate time usage of phase 2
                cntDisjointIntervals--;
            }

            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getExLower(), ex) == 0 && Double.compare(current.getEx2Lower(), ex2) == 0)) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getExLower(), ex);
                ex2 = Math.min(current.getEx2Lower(), ex2);
                betaPartitions = current.getBetaPartitions() | betaPartitions;
            } else {
                result.add(new NormInterval(start, end, ex, ex2, betaPartitions));
                cntOffsets += end - start + 1;
                start = current.getLeft();
                end = current.getRight();
                ex = current.getExLower();
                ex2 = current.getEx2Lower();
                betaPartitions = current.getBetaPartitions();
            }
        }
        result.add(new NormInterval(start, end, ex, ex2, betaPartitions));
        cntOffsets += end - start + 1;

        return new Pair<>(result, new Pair<>(cntDisjointIntervals, cntOffsets));
    }

    private List<NormInterval> sortAndMergeIntervals(List<NormInterval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingInt(NormInterval::getLeft));

        NormInterval first = intervals.get(0);
        int start = first.getLeft();
        int end = first.getRight();

        List<NormInterval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            NormInterval current = intervals.get(i);
            if (current.getLeft() - 1 <= end) {
                end = Math.max(current.getRight(), end);
            } else {
                result.add(new NormInterval(start, end));
                start = current.getLeft();
                end = current.getRight();
            }
        }
        result.add(new NormInterval(start, end));

        return result;
    }

    /**
     * Fetch the meta table of each KV-index
     */
    private void loadMetaTable() {
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) {
                statisticInfos.add(new ArrayList<>());
                continue;
            }
            try {
                statisticInfos.add(indexOperators[i].readStatisticInfo());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

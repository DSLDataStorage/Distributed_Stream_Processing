/*
 * Copyright 2017 Jiaye Wu
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

import kvmatch.common.entity.TimeSeriesNode;
import kvmatch.common.entity.rowkey.TimeSeriesRowKey;
import kvmatch.data.GaussianGenerator;
import kvmatch.data.RandomWalkGenerator;
import kvmatch.data.SeriesGenerator;
import kvmatch.data.SineGenerator;
import kvmatch.operator.TimeSeriesOperator;
import kvmatch.statistic.StatisticWriter;
import kvmatch.utils.RandomUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * A synthetic data generator
 * <p>
 * Created by Jiaye Wu on 16-8-9.
 */
public class DataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DataGenerator.class.getName());

    private static final String DATA_FILENAME_PREFIX = "files" + File.separator + "data-";

    private List<SeriesGenerator> generators = new ArrayList<>();
    private int dataLength;

    public DataGenerator(int dataLength) {
        this.dataLength = dataLength;
    }

    public static void main(String[] args) throws IOException {
        System.out.print("Data Length = ");
        Scanner scanner = new Scanner(System.in);
        int dataLength = scanner.nextInt();
        System.out.print("Generate data? [true/false] = ");
        boolean generateData = scanner.nextBoolean();
        scanner.close();

        DataGenerator generator = new DataGenerator(dataLength);
        if (generateData) {
            generator.generateSyntheticDataToFile();
        }
    }

    private double[] generateSegment(int maxLength) {
        int t = RandomUtils.random(0, generators.size() - 1);
        int l = RandomUtils.random(Math.min(1000, maxLength), maxLength);
        logger.info("Generate the following length-{} segment by generator-{}.", l, t);
        SeriesGenerator seriesGenerator = generators.get(t);
        return seriesGenerator.generate(l);
    }

    private void generateSyntheticDataToFile() {
        RandomWalkGenerator randomWalkGenerator = new RandomWalkGenerator(-5, 5, 0, 1);
        generators.add(randomWalkGenerator);
        GaussianGenerator gaussianGenerator = new GaussianGenerator(-5, 5, 0, 2);
        generators.add(gaussianGenerator);
        SineGenerator sineGenerator = new SineGenerator(2, 10, 2, 10, -5, 5);
        generators.add(sineGenerator);

        File file = new File(DATA_FILENAME_PREFIX + dataLength);
        try {
            FileUtils.forceMkdirParent(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            int left = dataLength;
            while (left > 0) {
                // generate the following segment by the generator
                double[] segment = generateSegment(Math.min(left, dataLength / 100));
                left -= segment.length;

                // write the segment to file
                for (double data : segment) {
                    System.out.println(data);
                    dos.writeDouble(data);
                    StatisticWriter.println(data + "");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Synthetic time series data is generated to file '{}'.", DATA_FILENAME_PREFIX + dataLength);
    }

    private void putData(TimeSeriesOperator timeSeriesOperator) throws IOException {
        long startTime = System.currentTimeMillis();

        TimeSeriesNode node = new TimeSeriesNode();
        TimeSeriesRowKey rowKey = new TimeSeriesRowKey(1);
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(DATA_FILENAME_PREFIX + dataLength)))) {
            for (int i = 1; i <= dataLength; i++) {
                double value = dis.readDouble();
                node.getData().add(value);
                logger.debug("Data #{} - {}", i, value);

                if (node.getData().size() == TimeSeriesNode.ROW_LENGTH || i == dataLength) {
                    timeSeriesOperator.writeTimeSeriesNode(rowKey, node);
                    node.getData().clear();
                    rowKey = new TimeSeriesRowKey(i + 1);
                }
            }
        } catch (EOFException e) {
            // do nothing
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }

        timeSeriesOperator.close();

        long endTime = System.currentTimeMillis();
        logger.info("Time usage: {} ms", endTime - startTime);
    }
}

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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

public class DataGenerator2 {
    private static final String DATA_FILENAME_PREFIX = "files" + File.separator + "data-";
    public void gen(int n, String file_name) throws IOException {
        File file = new File(DATA_FILENAME_PREFIX + n);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
        BufferedReader br = null;

        br = Files.newBufferedReader(Paths.get(file_name));
        String line = br.readLine();
        String first_array[] = line.split(",");
        double temp = Double.valueOf(first_array[0]);

        while((line = br.readLine())!=null)
        {
            String array[] = line.split(",");
            double data = Double.valueOf(array[0]);
            double Roc = data-temp;
            dos.writeDouble(Roc);
//            dos.writeDouble(data);
            temp = data;
        }
        br.close();
        dos.close();
    }

}

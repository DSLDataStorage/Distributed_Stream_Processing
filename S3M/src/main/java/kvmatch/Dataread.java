package kvmatch;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;

public class Dataread {
    public List<String> load(int offset, int length, String file_name) throws IOException {
        List<String> data = new ArrayList<>();
        BufferedReader br = Files.newBufferedReader(Paths.get(file_name));
        try{
            for(int i=0; i<offset-2; i++)
            {
                br.readLine();
            }
            String line = br.readLine();
            String first_array[] = line.split(",");
            double before_price = Double.valueOf(first_array[0]);
            for(int i=0; i<length; i++)
            {
                line = br.readLine();
                String array[] = line.split(",");
                double price = Double.valueOf(array[0]);
                data.add(String.valueOf(price - before_price));
                before_price = price;
//		data.add(String.valueOf(price));
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
        finally{
            br.close();
            return data;
        }
    }
}

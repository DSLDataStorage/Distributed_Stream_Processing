package kvmatch;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

public class Targetread {
    private static final String DATA_FILENAME_PREFIX = "files" + File.separator + "target-";
    public static void main(String[] args) throws IOException {
        System.out.print("Data Length = ");
        Scanner scanner = new Scanner(System.in);
        int dataLength = scanner.nextInt();
        // File file = new File(DATA_FILENAME_PREFIX + dataLength);
        File file = new File("files/index-18237-25");
        DataInputStream dos = new DataInputStream(new FileInputStream(file));

        try{
            while(true)
            {
            System.out.println(dos.readDouble());
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
        dos.close();
    }

}

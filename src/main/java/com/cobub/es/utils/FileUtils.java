
package com.cobub.es.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zqykj on 2017/5/8.
 */
public class FileUtils {

    public static List<String> readLineFile(String filePath) {
        BufferedReader bufferedReader = null;
        List<String> list = new ArrayList<String>();
        try {
            File file = new File(filePath);
            bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                list.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

}

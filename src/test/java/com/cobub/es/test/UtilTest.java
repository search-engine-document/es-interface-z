package com.cobub.es.test;

import com.cobub.es.utils.FileUtils;
import org.junit.Test;

import java.util.List;

/**
 * Created by alfer on 10/24/17.
 */
public class UtilTest {

    @Test
    public void read_file_hundard() {
        List<String> list = FileUtils.readLineFile("/local-data/toolbar/data/ci_1bai.txt");
        for (String l : list) {
            String[] fields = l.split("\t");
            System.out.println("lat=" + fields[4] + " ,lon=" + fields[5] + " ,addr=" + fields[8] + " ,city=" + fields[9]);
        }
        System.out.println(list.size());
    }

    @Test
    public void read_file_thousand() {
        List<String> list = FileUtils.readLineFile("/local-data/toolbar/data/ci_1qianwan.txt");
        long t1 = System.currentTimeMillis();
        for (String l : list) {
            String[] fields = l.split("\t");
            //System.out.println("lat=" + fields[4] + " ,lon=" + fields[5] + " ,addr=" + fields[8] + " ,city=" + fields[9]);
        }
        long t2 = System.currentTimeMillis();
        System.out.println(list.size());
        System.out.println("t2-t1=" + (t2 - t1));
    }

    @Test
    public void test_get_name(){
        System.out.println(UtilTest.class.getName());
    }

}

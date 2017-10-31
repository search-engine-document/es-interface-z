package com.cobub.es.test;

import com.cobub.es.client.ClientFactory;
import com.cobub.es.client.EsOperator;
import com.cobub.es.client.PutBean;
import com.cobub.es.json.JSONObject;
import com.cobub.es.utils.FileUtils;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by alfer on 10/24/17.
 */
public class LacCiTest {

    Client client;

    @Before
    public void init() {
        client = ClientFactory.getClient();
    }

    @After
    public void close() {
        if (null != client) {
            client.close();
        }
    }

    private List<String[]> getInfo(List<String> list) {
        List<String[]> laccis = new ArrayList<String[]>();
        for (String l : list) {
            String[] fields = l.split("\t");
            String[] ss = new String[4];
            ss[0] = fields[4];
            ss[1] = fields[5];
            ss[2] = fields[8];
            ss[3] = fields[9];
            laccis.add(ss);
            //System.out.println("lat=" + fields[4] + " ,lon=" + fields[5] + " ,addr=" + fields[8] + " ,city=" + fields[9]);
        }
        return laccis;
    }

    private List<String> getJsonObjList(List<String> list) {
        List<String> laccis = new ArrayList<String>();
        for (String l : list) {
            String[] fields = l.split("\t");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("lat", fields[4]);
            jsonObject.put("lon", fields[5]);
            jsonObject.put("addr", fields[8]);
            jsonObject.put("city", fields[9]);
            laccis.add(jsonObject.toString());
        }
        return laccis;
    }

    private List<PutBean> getPuts(String index, String type, List<String> list) {
        List<PutBean> puts = new ArrayList<PutBean>();
        for (String l : list) {
            String[] fields = l.split("\t");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("lat", fields[4]);
            jsonObject.put("lon", fields[5]);
            jsonObject.put("address_1234567890_abcdefghijklmn_opqrstuvwxyz_address", fields[9]);
            jsonObject.put("city", fields[9]);
            PutBean bean = new PutBean(index, type, jsonObject.toString());
            puts.add(bean);
        }
        return puts;
    }

    @Test
    public void lac_ci_write() {
        String index = "lac_ci";
        String type = "addrs";
        System.out.println("start reading...");
        List<String> list = FileUtils.readLineFile("/local-data/toolbar/data/ci_1baiwan.txt");
        List<PutBean> beanList = getPuts(index, type, list);
        list.clear();
        System.out.println("start writing...");

        int total = beanList.size();
        int num = 100000;
        EsOperator esOperator = new EsOperator(client);
        long t1 = System.currentTimeMillis();
        int item = total / num;
        System.out.println("item=" + item);
        for (int i = 0; i < item; i++) {
            long t3 = System.currentTimeMillis();
            List<PutBean> subList = beanList.subList(i * num, (i + 1) * num);
            esOperator.putList(subList);
            long t4 = System.currentTimeMillis();
            System.out.println(num + " use time=" + (t4 - t3) + " ,speed=" + (num / ((t4 - t3) / 1000f)));
        }
        if (total % num != 0) {
            esOperator.putList(beanList.subList(item * num, total));
        }
        long t2 = System.currentTimeMillis();
        System.out.println("t2-t1=" + ((t2 - t1) / 1000f) + " , speed=" + total / ((t2 - t1) / 1000f));

    }

}

package com.cobub.es.test;

import com.cobub.es.alias.IndexUpdate;
import com.cobub.es.client.*;
import com.cobub.es.common.Constants;
import com.cobub.es.json.JSONObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by feng.wei on 2015/12/17.
 */
public class InterfaceTest {

    static String[] citys = {"南京", "北京", "深圳", "上海", "广州", "连云港", "无锡", "大连", "西安", "西宁", "苏州", "杭州"};
    static String[] sexs = {"男", "女"};
    static String[] channels = {"百度", "华为", "小米", "九九畅游", "appstore", "豌豆荚", "360"};
    static String[] numbers = {"1", "2", "3", "4", "5", "6", "7", "8", "9"};
    static String index = "tag";
    static String type = "PeopleProperties";

    public String generateContent(Random random) {
        String phoneNumber = 1 + "";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("city", citys[random.nextInt(citys.length)]);
        jsonObject.put("sex", sexs[random.nextInt(sexs.length)]);
        jsonObject.put("channel", channels[random.nextInt(channels.length)]);
        for (int i = 0; i < 10; i++) {
            phoneNumber += numbers[random.nextInt(numbers.length)];
        }
        jsonObject.put("phoneNumber", phoneNumber);
        return jsonObject.toString();
    }

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

    @Test
    public void test_get_client() {
        System.out.println(client);
    }


    @Test
    public void test_get() {

        String user = EsSearch.getById(client, "tag", "PeopleProperties", "62537fb28266471eba334bb99f6e502a");
        System.out.println(user);


    }

    @Test
    public void test_searchByTerm() {
        Map<String, String> termMap = new HashMap<String, String>();
        termMap.put("city", "南京");
        termMap.put("channel", "华为");
        SearchHit[] hits = EsSearch.searchByTerm(client, "tag", "PeopleProperties", termMap, Constants.GATHER.MUST, 0, 10000);
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + ", " + hit.getSourceAsString());
        }
        System.out.println(hits.length);
    }

    @Test
    public void test_getUids() {
        Map<String, String> termMap = new HashMap<String, String>();
        termMap.put("city", "南京");
        termMap.put("channel", "华为");
        Set<String> sets = EsSearch.getUids(client, "tag", "PeopleProperties", termMap, Constants.GATHER.MUST, null, null);
        for (String s : sets) {
            System.out.println(s);
        }
        System.out.println(sets.size());

    }

    @Test
    public void test_deleteone() {
        EsOperator esOperator = new EsOperator();
        boolean flag = esOperator.delete("tag", "PeopleProperties", "62537fb28266471eba334bb99f6e502a");
        System.out.println(flag);
    }

    @Test
    public void test_isExist() {
        EsOperator esOperator = new EsOperator();
        boolean flag = esOperator.isExist("tag", "PeopleProperties", "111111");
        System.out.println(flag);

    }

    @Test
    public void test_put() {
        EsOperator esOperator = new EsOperator();
        PutBean putBean = new PutBean("tag", "PeopleProperties", "111111", generateContent(new Random()));
        esOperator.put(putBean);
    }

    @Test
    public void test_put_list() {
        EsOperator esOperator = new EsOperator(client);
        Random random = new Random();
        List<PutBean> list = new ArrayList<PutBean>();
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            PutBean putBean = new PutBean(index, type, UUID.randomUUID().toString().replace("-", ""), generateContent(random));
            list.add(putBean);
        }
        esOperator.putList(list);
        long t2 = System.currentTimeMillis();
        System.out.println((t2 - t1) + " ms");

    }

    @Test
    public void multi_thread_put() {
        final EsOperator esOperator = new EsOperator(client);
        final Random random = new Random();
        for (int n = 0; n < 5; n++) {
            new Thread(new Runnable() {
                List<PutBean> list = new ArrayList<PutBean>();

                public void run() {
                    for (int i = 0; i < 10000; i++) {
                        PutBean putBean = new PutBean(index, type, UUID.randomUUID().toString().replace("-", ""), generateContent(random));
                        list.add(putBean);
                    }
                    esOperator.putList(list);
                }
            }).start();
        }
    }

    @Test
    public void test_alias() {

        IndexUpdate.updateIndex(client, "razor_v1", "event", IndexUpdate.getDate(client, "razor", "event"));
    }

    @Test
    public void test_str() {
        String str1 = "weifengisgoodmanweifengwei";
        String str2 = "wei";

        String[] strs = str1.split(str2);
        System.out.println(strs.length);

        for (String s : strs) {
            System.out.println(s);
        }

    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Client client1 = ClientFactory.getClient();
        new Thread(new Runnable() {

            public void run() {
                SearchResponse searchResponse = null;
                long t1 = System.currentTimeMillis();
                try {
                    searchResponse = client1.prepareSearch()
                            .setIndices("razor_cd_v1")
                            .setTypes("201601")
                            .setSize(20000)
                            .setFrom(0)
                            .execute()
                            .get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                SearchHit[] hits = searchResponse.getHits().getHits();
                long t2 = System.currentTimeMillis();
                System.out.println(Thread.currentThread() + ", t2 - t1=" + ((t2 - t1) / 1000));
//                for (SearchHit hit : hits) {
//                    System.out.println(Thread.currentThread() + ", id = " + hit.getId());
//                }
            }

        }).start();


        new Thread(new Runnable() {

            public void run() {
                GetResponse getResponse =
                        null;
                try {
                    getResponse = client1.prepareGet("razor_cd_v1", "201601", "c586832e63f735089dbb659815ed7d26")
                            .execute().get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                System.out.println(Thread.currentThread() + ", id = " + getResponse.getId());
            }

        }).start();


        new Thread(new Runnable() {
            Client client1 = ClientFactory.getClient();

            public void run() {
                long t1 = System.currentTimeMillis();
                Map<String, String> termMap = new HashMap<String, String>();
                termMap.put("network", "WIFI");
                termMap.put("channelid", "1");
                termMap.put("useridentifier", "rtkit");
                SearchHit[] hits = EsSearch.searchByTerm(client1, "razor_cd_v1", "201601", termMap, Constants.GATHER.MUST, 0, 2000);
                long t2 = System.currentTimeMillis();
                System.out.println(Thread.currentThread() + ", t2 - t1=" + ((t2 - t1) / 1000));
//                for (SearchHit hit : hits) {
//                    System.out.println(Thread.currentThread() + ", " + hit.getId() + ", " + hit.getSourceAsString());
//                }
            }
        }).start();


    }

}

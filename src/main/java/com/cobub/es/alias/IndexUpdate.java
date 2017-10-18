package com.cobub.es.alias;

import com.cobub.es.client.ClientFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by feng.wei on 2015/12/31.
 */
public class IndexUpdate {

    Client client;

    public static List<Map> getDate(Client client, String index, String type) {
        List<Map> list = new ArrayList<Map>();
        // Client client = ClientFactory.getClient();
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                //.setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(100000))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(100)
                .setFrom(0)
                .execute()
                .actionGet();


        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        for (SearchHit hit : response.getHits()) {
            System.out.println("hit.source=" + hit.getSourceAsString());
            list.add(hit.getSource());
        }

        System.out.println("size=" + list.size());
        return list;
    }

    public static void updateIndex(Client client, String index, String type, List<Map> list) {

        BulkRequestBuilder bulk = client.prepareBulk();

        int size = list.size();
        int bulkLength = 50000;

        if (size <= bulkLength) {
            for (Map map : list) {
                bulk.add(client.prepareIndex(index, type).setSource(map));
            }
            bulk.execute();
        } else {
            int tmp = (size / bulkLength);
            int roar = (size % bulkLength);
            for (int i = 0; i < tmp; i++) {
                for (int j = (i * bulkLength); j < ((i + 1) * bulkLength); i++) {
                    bulk.add(client.prepareIndex(index, type).setSource(list.get(j)));
                }
                bulk.execute();
            }

            if (0 != roar) {
                for (int k = (tmp * bulkLength); k < size; k++) {
                    bulk.add(client.prepareIndex(index, type).setSource(list.get(k)));
                }
                bulk.execute();
            }
        }

    }


}

package com.cobub.es.client;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by feng.wei on 2015/12/17.
 */
public class EsOperator {

    Client client;


    public EsOperator() {
        client = ClientFactory.getClient();
    }

    public EsOperator(Client client) {
        this.client = client;
    }

    /**
     * batch put data into es
     *
     * @param beanList
     */
    public void putList(List<PutBean> beanList) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (PutBean putBean : beanList) {
            bulkRequestBuilder.add(prePut(putBean));
        }
        ListenableActionFuture<BulkResponse> future = bulkRequestBuilder.execute();
        System.out.println("status=" + future.actionGet().status().getStatus());

    }

    /**
     * put single data into es
     *
     * @param putBean
     * @return
     */
    public void put(PutBean putBean) {

        client.prepareIndex(putBean.getIndex(), putBean.getType(), putBean.getId())
                .setSource(putBean.getSource())
                .execute();

    }

    public IndexRequestBuilder prePut(PutBean putBean) {

        return client.prepareIndex(putBean.getIndex(), putBean.getType(), putBean.getId())
                .setSource(putBean.getSource());

    }


    /**
     * index/type/id wheather is exist.
     *
     * @param index
     * @param type
     * @param id
     * @return
     */
    public boolean isExist(String index, String type, String id) {
        boolean flag = false;
        try {
            flag = client.prepareGet(index, type, id).execute().get().isExists();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * delete a data according to index/type/id
     *
     * @param index
     * @param type
     * @param id
     * @return false if delete operation is successful, else true
     */
    public boolean delete(String index, String type, String id) {
        return client.prepareDelete(index, type, id).execute().isDone();
    }

}

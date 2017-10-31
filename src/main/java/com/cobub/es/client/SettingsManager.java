package com.cobub.es.client;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by alfer on 10/18/17.
 */
public class SettingsManager {

    IndicesAdminClient indicesAdminClient;

    public SettingsManager() {
        Client client = ClientFactory.getClient();
        indicesAdminClient = client.admin().indices();
    }

    /**
     * .put("index.number_of_shards", 3)
     * .put("index.number_of_replicas", 2)
     * <p>
     * The PUT mapping API allows you to add a new type while creating an index
     *
     * @param indice
     * @param builder
     */
    public void createIndex(String indice, Settings.Builder builder, String type, Map<String, Object> mappingSource) {
        indicesAdminClient.prepareCreate(indice)
                .setSettings(builder)
                .addMapping(type, mappingSource)
                .get();
    }

    public void createIndex(String indice, Settings.Builder builder) {
        indicesAdminClient.prepareCreate(indice)
                .setSettings(builder)
                .get();
    }

    public void createIndex(String indice) {
        createIndex(indice, Settings.builder());
    }


    /**
     * The PUT mapping API also allows to add a new type to an existing index
     * put or update the mapping
     *
     * @param indice
     * @param type
     */
    public void putMapping(String indice, String type, Map mappigSource) {
        indicesAdminClient.preparePutMapping(indice)
                .setType(type)
                .setSource(MappingBuilder.getMapping(mappigSource))
                .get();
    }

    public void putMapping(String indice, String type) {
        indicesAdminClient.preparePutMapping(indice)
                .setType(type)
                .setSource(MappingBuilder.getMapping())
                .get();
    }

    static class MappingBuilder {
        public static XContentBuilder getMapping(Map<String, Map<String, String>> map) {
            XContentBuilder mapping = null;
            try {
                mapping = jsonBuilder()
                        .startObject()
                        .startObject("properties");

                Iterator<Map.Entry<String, Map<String, String>>> propsIter = map.entrySet().iterator();
                while (propsIter.hasNext()) {
                    Map.Entry<String, Map<String, String>> mapEntry = propsIter.next();
                    mapping.startObject(mapEntry.getKey());
                    Iterator<Map.Entry<String, String>> iterator = mapEntry.getValue().entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> entry = iterator.next();
                        mapping.field(entry.getKey(), entry.getValue());
                    }
                    mapping.endObject();
                }
                mapping.endObject()
                        .endObject();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return mapping;
        }

        public static XContentBuilder getMapping() {
            XContentBuilder mapping = null;
            try {
                mapping = jsonBuilder()
                        .startObject()
//                        //_ttl has removed in 5.0
                        .startObject("properties")
                        //                        .startObject("id").field("type", "long").endObject()
                        //                        .startObject("title").field("type", "string").field("store", "yes")
                        //                        //指定index analyzer 为 ik
                        //                        .field("analyzer", "ik")
                        //                        //指定search_analyzer 为ik_syno
                        //                        .field("searchAnalyzer", "ik_syno")
                        //                        .endObject()
                        .startObject("sex").field("type", "string")
                        .endObject()
                        .startObject("city").field("type", "string")
                        .endObject()
                        .startObject("channel").field("type", "string").field("index", "not_analyzed")
                        .endObject()
                        .startObject("phoneNumber").field("type", "string")
                        .endObject()
                        .endObject()
                        .endObject();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return mapping;
        }
    }

    /**
     * The refresh API allows to explicitly refresh one or more index
     *
     * @param indices
     */
    public void refreshIndices(String... indices) {
        indicesAdminClient.prepareRefresh().get();
    }


    public GetSettingsResponse getSettings(String... indices) {
        GetSettingsResponse response = indicesAdminClient
                .prepareGetSettings("company", "employee")
                .get();
        return response;
    }

    public String getSetting(String indice, String params) {
        return getSettings(indice).getSetting(indice, params);
    }

    public void fetchSettingsValues(GetSettingsResponse response) {
        for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
            String index = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);
        }
    }

}

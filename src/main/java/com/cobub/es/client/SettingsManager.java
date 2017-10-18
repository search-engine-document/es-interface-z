package com.cobub.es.client;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

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
                .setSource(mappigSource)
                .get();
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

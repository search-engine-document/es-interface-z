package com.cobub.es.test;

import com.cobub.es.client.SettingsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by alfer on 10/18/17.
 */
public class SettingsTest {

    SettingsManager settingsManager;

    @Before
    public void init() {
        settingsManager = new SettingsManager();
    }

    @After
    public void close() {
    }

    @Test
    public void create_index() {
        settingsManager.createIndex("twitter");
    }

    @Test
    public void create_type_mapping() {
        settingsManager.putMapping("twitter", "tagType");
    }

    @Test
    public void create_type_mapping2() {
        Map<String, Map<String, String>> propsMap = new HashMap<String, Map<String, String>>();
        Map<String, String> filedMap = new HashMap<String, String>();
        filedMap.put("type", "text");
        filedMap.put("index", "not_analyzed");
        filedMap.put("analyzer", "standard");
        propsMap.put("title", filedMap);
        settingsManager.putMapping("twitter", "autoType", propsMap);

    }
}

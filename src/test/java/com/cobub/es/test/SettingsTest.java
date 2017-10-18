package com.cobub.es.test;

import com.cobub.es.client.SettingsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
}

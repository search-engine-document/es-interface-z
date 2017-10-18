package com.cobub.es.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Created by feng.wei on 2015/11/13.
 */
public class ClientFactory {

    private Client client = null;
    private static Properties prop;

    static {
        InputStream in = ClientFactory.class.getResourceAsStream("/es-conf.properties");

        try {
            prop = new Properties();
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Client getClient() {
        Client client = null;
        try {

            Settings settings = Settings.builder()
                    .put("cluster.name", prop.getProperty("cluster.name", "zqy-es-application"))
                    .put("client.transport.sniff", prop.getProperty("client.transport.sniff", "true"))  // true 把集群中其它机器的ip地址加到客户端中
                    .build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(
                            new InetSocketTransportAddress(InetAddress.getByName("dev60"), 9300)
                    );

        } catch (UnknownHostException e) {
            System.out.println("unkown host: " + e.getMessage());
        } finally {

            return client;
        }
    }

    public void close() {
        if (null != client) {
            client.close();
        }
    }

}

package com.cobub.es.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

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

    public static void main(String[] args) {
        System.out.println(prop.getProperty("cluster.name"));
        System.out.println(prop.getProperty("client.transport.sniff"));
    }

    public static Client getClient() {
        Client client = null;
        try {

            client = TransportClient.builder().settings(
                    Settings.builder().put("cluster.name", prop.getProperty("cluster.name", "zqy-es-application"))
                            .put("client.transport.sniff", prop.getProperty("client.transport.sniff", "true"))  // true 把集群中其它机器的ip地址加到客户端中
//                            .put("shield.user", prop.getProperty("shield.user","es@admin:es@admin"))  // Using the Java Node Client with Shield
//                            .put("plugin.types", prop.getProperty("plugin.types","org.elasticsearch.shield.ShieldPlugin"))
            ).build()
                    .addTransportAddresses(
                            new InetSocketTransportAddress(InetAddress.getByName("dev60"), 9300)
                    )
            ;

//            client = TransportClient.builder().settings(Settings.builder().put("cluster.name", "elasticsearch")).build()
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            System.out.println("unkown host: " + e.getMessage());
        } finally {

            return client;
        }
    }

    public static Client getClientByNode() {
        Node node = NodeBuilder.nodeBuilder().clusterName("zqy-es-application")
                .settings(Settings.builder().put("cluster.name", "zqy-es-application"))
                .node();
        Client client1 = node.client();
        return client1;
    }

    public void close() {
        if (null != client) {
            client.close();
        }
    }

}

package io.streamnative.pulsar.handlers.mqtt;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.net.URI;

public class Test {

    public static void main(String[] args) throws Exception{
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", 1883);
        byte[] dataBytes = new byte[65536];
        int count = 1000000;
        long time = System.currentTimeMillis();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        while (true) {
            connection.publish("tp1", dataBytes, QoS.AT_LEAST_ONCE, false);
            if (count % 1000 == 0) {
                long timeUse = System.currentTimeMillis() - time;
                System.out.println("time use " + timeUse + ", speed: " + (1000 * 1000 / timeUse));
                time = System.currentTimeMillis();
            }
        }
    }
}

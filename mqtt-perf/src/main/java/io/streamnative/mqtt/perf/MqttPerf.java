package io.streamnative.mqtt.perf;

import static java.lang.System.exit;

import picocli.CommandLine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class MqttPerf {
    public static final int MQTT_VERSION_3 = 3;
    public static final int MQTT_VERSION_5 = 5;
    public static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors() * 2);
    public static final ExecutorService ASYNC_EXECUTOR = Executors.newCachedThreadPool();

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }

    public static void main(String[] args) {
        exit(new CommandLine(new CommandPerf()).execute(args));
    }
}

package io.streamnative.mqtt.perf;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.streamnative.mqtt.perf.MqttPerf.*;
import static io.streamnative.mqtt.perf.MqttPerf.LOG;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@CommandLine.Command(name = "sub", description = "subscribe messages to the broker")
public final class CommandSub implements Runnable {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;
    @CommandLine.Option(names = {"-h", "--host"}, defaultValue = "127.0.0.1",
            description = "MQTT Server host")
    String host;
    @CommandLine.Option(names = {"-p", "--port"}, defaultValue = "1883",
            description = "MQTT Server port")
    int port;
    @CommandLine.Option(names = {"-v", "--version"}, defaultValue = "5",
            description = "MQTT Protocol version")
    int version;
    @CommandLine.Option(names = {"-c", "--connections"}, defaultValue = "1",
            description = "MQTT Connection number")
    int connections;
    @CommandLine.Option(required = true, names = {"-t", "--topic"}, description = "Topic name")
    String topic;
    @CommandLine.Option(names = {"--topic-suffix"}, description = "Topic name suffix")
    CommandLine.Range topicSuffix;
    @CommandLine.Option(names = {"-q", "--qos"}, defaultValue = "1", description = "MQTT publish Qos")
    int qos;
    @CommandLine.Option(names = {"--auto-reconnect"}, description = "Enable auto reconnect.")
    boolean autoReconnect;
    @CommandLine.Option(names = {"--ssl-enabled"}, description = "Enable auto reconnect.")
    boolean sslEnabled;
    @CommandLine.Option(names = {"--username"}, description = "MQTT basic authentication username")
    String username;
    @CommandLine.Option(names = {"--password"}, description = "MQTT basic authentication UTF-8 encoded password")
    String password;

    @SuppressWarnings({"ResultOfMethodCallIgnored", "UnstableApiUsage"})
    @Override
    public void run() {
        // arguments validation
        if (topicSuffix != null) {
            final var steps = topicSuffix.max() - topicSuffix.min();
            if (steps != connections) {
                throw new CommandLine.ParameterException(spec.commandLine(), "connection number must equals to topic suffix ranges");
            }
        }
        LOG.info("Preparing the publisher configurations.");

        final var clientBuilder = MqttClient.builder()
                .serverHost(host)
                .serverPort(port)
                .sslWithDefaultConfig();
        if (autoReconnect) {
            clientBuilder.automaticReconnectWithDefaultConfig();
        }
        if (sslEnabled) {
            clientBuilder.sslWithDefaultConfig();
        }
        final var clientId = String.valueOf(System.currentTimeMillis());

        switch (version) {// blocking call and wait for all the client connect successful.
            case MQTT_VERSION_5:
                LOG.info("Preparing the MQTT 5 connection.");
                final var connectFutures = IntStream.range(0, connections).parallel().mapToObj(index -> {
                    String concat = clientId.concat(String.valueOf(index));
                    System.out.println(concat);
                    final var mqtt5AsyncClient = clientBuilder
                            .identifier(concat)
                            .useMqttVersion5().buildAsync();
                    final var connectBuilder = mqtt5AsyncClient.connectWith();
                    if (!Strings.isNullOrEmpty(password)) {
                        connectBuilder
                                .simpleAuth()
                                .username(username)
                                .password(password.getBytes(StandardCharsets.UTF_8))
                                .applySimpleAuth();
                    }
                    return connectBuilder.send().thenApply(__ -> mqtt5AsyncClient);
                }).collect(Collectors.toList());
                allOf(connectFutures.toArray(new CompletableFuture[]{})).join();
                LOG.info("Preparing publishing messages.");
            default:
                throw new CommandLine.ParameterException(spec.commandLine(), "Unsupported protocol version " + version);
        }

    }
}

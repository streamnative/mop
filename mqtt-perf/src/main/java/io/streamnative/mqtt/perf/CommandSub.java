package io.streamnative.mqtt.perf;

import static io.streamnative.mqtt.perf.MqttPerf.EXECUTOR;
import static io.streamnative.mqtt.perf.MqttPerf.MQTT_VERSION_5;
import static io.streamnative.mqtt.perf.MqttPerf.TF;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.annotation.JSONField;
import com.google.common.base.Strings;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import picocli.CommandLine;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@CommandLine.Command(name = "sub", description = "subscribe messages to the broker")
public final class CommandSub implements Runnable {
    private static final Logger LOG = Logger.getLogger("Subscriber");

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

    // =================


    private final LongAdder intervalMessagesReceived = new LongAdder();
    private final LongAdder intervalBytesReceived = new LongAdder();
    private final LongAdder intervalMessageAcked = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();
    private final LongAdder totalBytesReceived = new LongAdder();
    private final LongAdder totalMessageAck = new LongAdder();
    private final LongAdder connected = new LongAdder();
    private final LongAdder subscribed = new LongAdder();
    private final AtomicLong lastPrintTime = new AtomicLong(0);
    private final CompletableFuture<Void> exitFuture = new CompletableFuture<>();

    @SneakyThrows
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    @Override
    public void run() {
        printStatsInBackgoundByInterval();
        // arguments validation
        if (topicSuffix != null) {
            final var steps = topicSuffix.max() - topicSuffix.min();
            if (steps != connections) {
                throw new CommandLine.ParameterException(spec.commandLine(),
                        "connection number must equals to topic suffix ranges");
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
        final var clientId = UUID.randomUUID().toString();
        switch (version) {// blocking call and wait for all the client connect successful.
            case MQTT_VERSION_5:
                LOG.info("Preparing the MQTT 5 connection.");
                final var connectFutures = IntStream.range(0, connections).parallel().mapToObj(index -> {
                    final var mqtt5ClientBuilder = clientBuilder
                            .useMqttVersion5()
                            .identifier(clientId.concat(String.valueOf(index)));
                    if (!Strings.isNullOrEmpty(password)) {
                        mqtt5ClientBuilder
                                .simpleAuth()
                                .username(username)
                                .password(password.getBytes(StandardCharsets.UTF_8))
                                .applySimpleAuth();
                    }
                    final var mqtt5AsyncClient = mqtt5ClientBuilder.buildAsync();
                    return mqtt5AsyncClient.connect().thenApply(__ -> {
                        connected.increment();
                        return mqtt5AsyncClient;
                    });
                }).collect(Collectors.toList());
                allOf(connectFutures.toArray(new CompletableFuture[]{})).join();
                LOG.info("Preparing subscription.");
                final var futures = new ArrayList<>();
                for (var future : connectFutures) {
                    final var mqtt5AsyncClient = future.join();
                    final var subFuture = mqtt5AsyncClient.subscribeWith()
                            .topicFilter(topic)
                            .qos(Optional.ofNullable(MqttQos.fromCode(qos)).orElse(MqttQos.AT_LEAST_ONCE))
                            .callback(publish -> {
                                intervalMessagesReceived.increment();
                                totalMessagesReceived.increment();
                                final var bytes = publish.getPayloadAsBytes().length;
                                intervalBytesReceived.add(bytes);
                                totalBytesReceived.add(bytes);
                                intervalMessageAcked.increment();
                                totalMessageAck.increment();
                            })
                            .send().thenAccept(__ -> subscribed.increment());
                    futures.add(subFuture);
                }
                allOf(futures.toArray(new CompletableFuture[]{})).join();
                exitFuture.join();
            default:
                throw new CommandLine.ParameterException(spec.commandLine(), "Unsupported protocol version " + version);
        }
    }

    private void printStatsInBackgoundByInterval() {
        EXECUTOR.scheduleWithFixedDelay(() -> {
            final var printTime = System.nanoTime();
            final var elapsed = (printTime - lastPrintTime.get()) / 1e9;
            final var receivedRate = intervalMessagesReceived.sumThenReset() / elapsed;
            final var throughput = intervalBytesReceived.sumThenReset() / elapsed;
            final var ackRate = intervalMessageAcked.sumThenReset() / elapsed;
            final var stats = SubStats.builder()
                    .total_message_received(totalMessagesReceived.longValue())
                    .connected(connected.longValue())
                    .subscribed(subscribed.longValue())
                    .receive_rate_per_second(TF.format(receivedRate))
                    .throughput_bytes(TF.format(throughput))
                    .ack_rate_per_second(TF.format(ackRate))
                    .build().toString();
            LOG.info(stats);
            lastPrintTime.set(System.nanoTime());
        }, 10, 10, SECONDS);
    }


    @Data
    @Builder
    static class SubStats {
        @JSONField(ordinal = 1)
        private long total_message_received;
        @JSONField(ordinal = 2)
        private long total_message_acked;
        @JSONField(ordinal = 3)
        private String receive_rate_per_second;
        @JSONField(ordinal = 4)
        private String throughput_bytes;
        @JSONField(ordinal = 5)
        private String ack_rate_per_second;
        @JSONField(ordinal = 6)
        private long connected;
        @JSONField(ordinal = 7)
        private long subscribed;

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
}

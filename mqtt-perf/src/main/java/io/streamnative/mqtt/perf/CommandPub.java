package io.streamnative.mqtt.perf;

import static io.streamnative.mqtt.perf.MqttPerf.*;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Model;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.ParameterException;
import static picocli.CommandLine.Range;
import static picocli.CommandLine.Spec;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.annotation.JSONField;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import lombok.Builder;
import lombok.Data;
import org.HdrHistogram.Recorder;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Command(name = "pub", description = "publish messages to the broker")
public final class CommandPub implements Runnable {
    @Spec
    Model.CommandSpec spec;
    @Option(names = {"-h", "--host"}, defaultValue = "127.0.0.1",
            description = "MQTT Server host")
    String host;
    @Option(names = {"-p", "--port"}, defaultValue = "1883",
            description = "MQTT Server port")
    int port;
    @Option(names = {"-v", "--version"}, defaultValue = "5",
            description = "MQTT Protocol version")
    int version;
    @Option(names = {"-c", "--connections"}, defaultValue = "1",
            description = "MQTT Connection number")
    int connections;
    @Option(names = {"-r", "--rate"}, defaultValue = "100",
            description = "MQTT Publish rate per second by connection")
    long rate;
    @Option(required = true, names = {"-t", "--topic"}, description = "Topic name")
    String topic;
    @Option(names = {"--topic-suffix"}, description = "Topic name suffix")
    Range topicSuffix;
    @Option(names = {"-q", "--qos"}, defaultValue = "1", description = "MQTT publish Qos")
    int qos;
    @Option(names = {"-n", "--message-number"}, description = "Number of messages per connection")
    int messageNumber;
    @Option(names = {"-s", "--message-size"}, defaultValue = "128",
            description = "MQTT message size")
    int messageSize;
    @Option(names = {"--auto-reconnect"}, description = "Enable auto reconnect.")
    boolean autoReconnect;
    @Option(names = {"--ssl-enabled"}, description = "Enable auto reconnect.")
    boolean sslEnabled;
    @Option(names = {"--username"}, description = "MQTT basic authentication username")
    String username;
    @Option(names = {"--password"}, description = "MQTT basic authentication UTF-8 encoded password")
    String password;


    // ============================= metrics ========================

    private final LongAdder totalMessageSent = new LongAdder();
    private final LongAdder intervalMessageSent = new LongAdder();
    private final LongAdder intervalMessageFailed = new LongAdder();
    private final LongAdder intervalBytesSent = new LongAdder();
    private final Recorder recorder = new Recorder(SECONDS.toMicros(120000), 5);
    private final AtomicLong lastPrintTime = new AtomicLong(0);


    {
        EXECUTOR.scheduleWithFixedDelay(() -> {
            final var printTime = System.nanoTime();
            final var elapsed = (printTime - lastPrintTime.get()) / 1e9;
            final var publishRate = intervalMessageSent.sumThenReset() / elapsed;
            final var publishFailedRate = intervalMessageFailed.sumThenReset() / elapsed;
            final var throughput = intervalBytesSent.sumThenReset() / elapsed;
            final var histogram = recorder.getIntervalHistogram();
            final var latency = PubStats.PublishLatency.builder()
                    .mean(DF.format(histogram.getMean() / 1000.0))
                    .pct50(DF.format(histogram.getValueAtPercentile(50) / 1000.0))
                    .pct95(DF.format(histogram.getValueAtPercentile(95) / 1000.0))
                    .pct99(DF.format(histogram.getValueAtPercentile(99) / 1000.0))
                    .pct999(DF.format(histogram.getValueAtPercentile(99.9) / 1000.0))
                    .pct9999(DF.format(histogram.getValueAtPercentile(99.99) / 1000.0))
                    .max(DF.format(histogram.getMaxValue() / 1000.0))
                    .build();
            final var stats = PubStats.builder()
                    .total_message_sent(totalMessageSent.sum())
                    .publish_failed_rate_per_second(TF.format(publishFailedRate))
                    .publish_rate_per_second(TF.format(publishRate))
                    .throughput_bytes(TF.format(throughput))
                    .latency_ms(latency)
                    .build().toString();
            LOG.info(stats);
            lastPrintTime.set(System.nanoTime());
        }, 10, 10, SECONDS);
    }

    @SuppressWarnings({"ResultOfMethodCallIgnored", "UnstableApiUsage"})
    @Override
    public void run() {
        // arguments validation
        if (topicSuffix != null) {
            final var steps = topicSuffix.max() - topicSuffix.min();
            if (steps != connections) {
                throw new ParameterException(spec.commandLine(), "connection number must equals to topic suffix ranges");
            }
        }
        LOG.info("Preparing the publisher configurations.");
        final var rateLimiters = new ConcurrentHashMap<String, RateLimiter>(connections);
        final var permits = new ConcurrentHashMap<String, AtomicLong>(connections);
        final var lastSentFutures = new ConcurrentHashMap<String, CompletableFuture<Void>>();

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

        final byte[] payload = new byte[messageSize];
        switch (version) {// blocking call and wait for all the client connect successful.
            case MQTT_VERSION_5:
                LOG.info("Preparing the MQTT 5 connection.");
                final var connectFutures = IntStream.range(0, connections).parallel().mapToObj(index -> {
                    final var mqtt5AsyncClient = clientBuilder
                            .identifier(clientId.concat(String.valueOf(index)))
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
                while (true) { // control number of messages
                    for (var connect : connectFutures) {
                        ASYNC_EXECUTOR.submit(() -> {
                            final var mqtt5Client = connect.join();
                            final var clientIdentifier = mqtt5Client.getConfig().getClientIdentifier();
                            assert clientIdentifier.isPresent();
                            if (messageNumber > 0) {
                                final var permit = permits.computeIfAbsent(clientIdentifier.get().toString(),
                                        key -> new AtomicLong(messageNumber));
                                if (permit.decrementAndGet() == 0) {
                                    return;
                                }
                            }
                            final var rateLimiter = rateLimiters.computeIfAbsent(clientIdentifier.get().toString(),
                                    key -> RateLimiter.create(rate));
                            if (!rateLimiter.tryAcquire()) {
                                return;
                            }
                            final long publishStartTime = System.nanoTime();
                            final var future = mqtt5Client.publishWith()
                                    .topic(topic)
                                    .qos(Optional.ofNullable(MqttQos.fromCode(qos)).orElse(MqttQos.AT_LEAST_ONCE))
                                    .payload(payload)
                                    .send().thenAccept(__ -> {
                                        final long publishEndTime = System.nanoTime();
                                        final long microsPublishedLatency = NANOSECONDS.toMicros(publishEndTime - publishStartTime);
                                        totalMessageSent.increment();
                                        intervalMessageSent.increment();
                                        intervalBytesSent.add(payload.length);
                                        recorder.recordValue(microsPublishedLatency);
                                    }).exceptionally(ex -> {
                                        intervalMessageFailed.increment();
                                        return null;
                                    });
                            lastSentFutures.put(clientIdentifier.get().toString(), future);
                        });
                    }
                    // check message number
                    if (messageNumber > 0 && permits.values().stream().mapToLong(AtomicLong::get).sum() == 0) {
                        LOG.info(messageNumber * connections + " messages have been posted, end the processes.");
                        break;
                    }
                }
                allOf(lastSentFutures.values().toArray(new CompletableFuture[]{})).join();
                break;
            default:
                throw new ParameterException(spec.commandLine(), "Unsupported protocol version " + version);
        }

    }

    @Data
    @Builder
    static class PubStats {
        @JSONField(ordinal = 1)
        private long total_message_sent;
        @JSONField(ordinal = 2)
        private String publish_rate_per_second;
        @JSONField(ordinal = 3)
        private String throughput_bytes;
        @JSONField(ordinal = 4)
        private String publish_failed_rate_per_second;
        @JSONField(ordinal = 5)
        private PublishLatency latency_ms;

        @Data
        @Builder
        static class PublishLatency {
            @JSONField(ordinal = 1)
            private String mean;
            @JSONField(ordinal = 2)
            private String pct50;
            @JSONField(ordinal = 3)
            private String pct95;
            @JSONField(ordinal = 4)
            private String pct99;
            @JSONField(ordinal = 5)
            private String pct999;
            @JSONField(ordinal = 6)
            private String pct9999;
            @JSONField(ordinal = 7)
            private String max;

            @Override
            public String toString() {
                return JSON.toJSONString(this);
            }
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
}

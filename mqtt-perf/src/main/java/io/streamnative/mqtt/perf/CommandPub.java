package io.streamnative.mqtt.perf;

import static io.streamnative.mqtt.perf.MqttPerf.EXECUTOR;
import static io.streamnative.mqtt.perf.MqttPerf.MQTT_VERSION_5;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.annotation.JSONField;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import lombok.Builder;
import lombok.Data;
import org.HdrHistogram.Recorder;
import picocli.CommandLine;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import java.util.stream.IntStream;

@CommandLine.Command(name = "pub", description = "publish messages to the broker")
public final class CommandPub implements Runnable {
    static final Logger LOG = Logger.getLogger("Publisher");
    static final DecimalFormat DF = new DecimalFormat("0.000");
    static final DecimalFormat TF = new DecimalFormat("0.0");
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
    @CommandLine.Option(names = {"-r", "--rate"}, defaultValue = "100",
            description = "MQTT Publish rate per second by connection")
    long rate;
    @CommandLine.Option(required = true, names = {"-t", "--topic"}, description = "Topic name")
    String topic;
    @CommandLine.Option(names = {"-q", "--q"}, defaultValue = "1", description = "MQTT publish Qos")
    int qos;
    @CommandLine.Option(names = {"-n", "--message-number"}, description = "Number of messages per connection")
    int messageNumber;
    @CommandLine.Option(names = {"-s", "--message-size"}, defaultValue = "128",
            description = "MQTT message size")
    int messageSize;
    @CommandLine.Option(names = {"--auto-reconnect"}, description = "Enable auto reconnect.")
    boolean autoReconnect;
    @CommandLine.Option(names = {"--ssl-enabled"}, description = "Enable auto reconnect.")
    boolean sslEnabled;
    @CommandLine.Option(names = {"--username"}, description = "MQTT basic authentication username")
    String username;
    @CommandLine.Option(names = {"--password"}, description = "MQTT basic authentication UTF-8 encoded password")
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
        LOG.info("Preparing the publisher configurations.");
        final var rateLimiters = new ConcurrentHashMap<String, RateLimiter>(connections);
        final var permits = new ConcurrentHashMap<String, AtomicLong>(connections);
        final var lastSentFutures = new ConcurrentHashMap<String, CompletableFuture<Void>>();

        final var clientBuilder = MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(host)
                .serverPort(port)
                .sslWithDefaultConfig();
        if (autoReconnect) {
            clientBuilder.automaticReconnectWithDefaultConfig();
        }
        if (sslEnabled) {
            clientBuilder.sslWithDefaultConfig();
        }
        final byte[] payload = new byte[messageSize];
        switch (version) {
            case MQTT_VERSION_5 -> {
                LOG.info("Preparing the MQTT 5 connection.");
                final var connectFutures = IntStream.range(0, connections).parallel().mapToObj(id -> {
                    final var mqtt5AsyncClient = clientBuilder.useMqttVersion5().buildAsync();
                    final var connectBuilder = mqtt5AsyncClient.connectWith();
                    if (!Strings.isNullOrEmpty(password)) {
                        connectBuilder
                                .simpleAuth()
                                .username(username)
                                .password(password.getBytes(StandardCharsets.UTF_8))
                                .applySimpleAuth();
                    }
                    return connectBuilder.send().thenApply(__ -> mqtt5AsyncClient);
                }).toList();
                // blocking call and wait for all the client connect successful.
                allOf(connectFutures.toArray(new CompletableFuture[]{})).join();
                LOG.info("Preparing publishing messages.");
                while (true) {
                    for (var connect : connectFutures) {
                        final var mqtt5Client = connect.join();
                        final var clientIdentifier = mqtt5Client.getConfig().getClientIdentifier();
                        assert clientIdentifier.isPresent();
                        if (messageNumber > 0) {
                            final var permit = permits.computeIfAbsent(clientIdentifier.get().toString(),
                                    key -> new AtomicLong(messageNumber));
                            if (permit.decrementAndGet() == 0) {
                                continue;
                            }
                        }
                        final var rateLimiter = rateLimiters.computeIfAbsent(clientIdentifier.get().toString(),
                                key -> RateLimiter.create(rate));
                        rateLimiter.acquire();
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
                    }
                    // check message number
                    if (messageNumber > 0 && permits.values().stream().mapToLong(AtomicLong::get).sum() == 0) {
                        LOG.info(messageNumber * connections + " messages have been posted, end the processes.");
                        break;
                    }
                }
                // wait for all the future complete
                allOf(lastSentFutures.values().toArray(new CompletableFuture[]{})).join();
            }
            default -> {
                throw new CommandLine.ParameterException(spec.commandLine(), "Unsupported protocol version " + version);
            }
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

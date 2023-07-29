package io.streamnative.mqtt.perf;

import picocli.CommandLine;

@CommandLine.Command(name = "sub", description = "subscribe messages to the broker")
public final class CommandSub {

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


}

package io.streamnative.mqtt.perf;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public final class MqttPerfFormatter extends Formatter {
    private final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");

    @Override
    public String format(LogRecord record) {
        final var builder = new StringBuilder();
        builder.append("[");
        builder.append(DF.format(new Date(record.getMillis())));
        builder.append("] ");
        builder.append(record.getMessage());
        builder.append("\n");
        return builder.toString();
    }
}
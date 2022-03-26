package io.streamnative.pulsar.handlers.mqtt.event;

import io.streamnative.pulsar.handlers.mqtt.utils.EventParserUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.naming.TopicName;
import java.util.regex.Pattern;

public interface PulsarTopicChangeListener extends PulsarEventListener {
    String MANAGED_LEDGER_PATH = "/managed-ledgers";
    String REGEX = MANAGED_LEDGER_PATH + "/" + ".*" + "/persistent/";
    Pattern PATTERN = Pattern.compile(REGEX);

    @Override
    default boolean matchPath(String path) {
        String listenNamespace = getListenNamespace();
        if (!StringUtils.isEmpty(listenNamespace)){
            return path.startsWith(MANAGED_LEDGER_PATH + "/" + listenNamespace + "/persistent/");
        } else {
            return PATTERN.matcher(path).find();
        }
    }

    default String getListenNamespace() {
        return "";
    }

    @Override
    default void onNodeCreated(String path) {
        TopicName topicName = EventParserUtils.parseFromManagedLedgerEvent(path);
        onTopicLoad(topicName);
    }

    @Override
    default void onNodeDeleted(String path) {
        TopicName topicName = EventParserUtils.parseFromManagedLedgerEvent(path);
        onTopicUnload(topicName);
    };

    void onTopicLoad(TopicName topicName);

    void onTopicUnload(TopicName topicName);
}

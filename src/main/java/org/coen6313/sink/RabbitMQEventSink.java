package org.coen6313.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQEventSink {

    // 默认配置
    private static final String HOST = "20.151.88.17"; // RabbitMQ 服务器 IP
    private static final int PORT = 5672;
    private static final String USERNAME = "coen6313";
    private static final String PASSWORD = "coen6313";
    private static final String VIRTUAL_HOST = "/";
    private static final String DEFAULT_QUEUE = "inactive-warnings";

    public static RMQSink<String> build() {
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(HOST)
                .setPort(PORT)
                .setUserName(USERNAME)
                .setPassword(PASSWORD)
                .setVirtualHost(VIRTUAL_HOST)
                .build();

        return new RMQSink<>(
                connectionConfig,
                DEFAULT_QUEUE,
                new SimpleStringSchema()
        );
    }

    public static RMQSink<String> build(String host, int port, String username,
                                        String password, String queueName) {
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(host)
                .setPort(port)
                .setUserName(username)
                .setPassword(password)
                .setVirtualHost(VIRTUAL_HOST)
                .build();

        return new RMQSink<>(
                connectionConfig,
                queueName,
                new SimpleStringSchema()
        );
    }
}

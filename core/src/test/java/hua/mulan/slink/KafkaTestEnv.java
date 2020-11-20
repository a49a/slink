package hua.mulan.slink;

import kafka.common.KafkaException;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.networking.NetworkFailuresProxy;
import org.apache.flink.util.NetUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.ArraySeq;

import java.io.File;
import java.net.BindException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/28
 **/
public class KafkaTestEnv {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestEnv.class);

    private File tmpZkDir;
    private File tmpKafkaParent;
    private TestingServer zookeeper;
    private Properties standardProps;
    private int zkTimeout = 30000;
    private String zookeeperConnectionString;
    private String brokerConnectionString;
    private List<File> tmpKafkaDirs;
    private int KAFKA_SERVER_NUMBER = 3;
    protected static final String KAFKA_HOST = "localhost";
    private final List<KafkaServer> brokers = new ArrayList<>();
    private Config config;
    private static final int DELETE_TIMEOUT_SECONDS = 30;


    public String getVersion() {
        return "2.0";
    }

    public String getBrokerConnectionString() {
        return brokerConnectionString;
    }

    public Properties getStandardProperties() {
        return standardProps;
    }

    public void prepare(Config config) throws Exception {
        this.config = config;

        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
        assertTrue("cannot create zookeeper temp dir", tmpZkDir.mkdirs());
        tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir-" + (UUID.randomUUID().toString()));
        assertTrue("cannot create kafka temp dir", tmpKafkaParent.mkdirs());

        zookeeper = new TestingServer(-1, tmpZkDir);
        zookeeperConnectionString = zookeeper.getConnectString();

        LOG.info("Starting Zookeeper with zookeeperConnectionString: {}", zookeeperConnectionString);

        LOG.info("Starting KafkaServer");

        ListenerName listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);

        tmpKafkaDirs = new ArrayList<>(KAFKA_SERVER_NUMBER);
        for (int i = 0; i < KAFKA_SERVER_NUMBER; i++) {
            File tmpDir = new File(tmpKafkaParent, "server-" + i);
            assertTrue("cannot create kafka temp dir", tmpDir.mkdir());
            tmpKafkaDirs.add(tmpDir);
        }
        for (int i = 0; i < KAFKA_SERVER_NUMBER; i++) {
            KafkaServer kafkaServer = getKafkaServer(i, tmpKafkaDirs.get(i));
            brokers.add(kafkaServer);
            brokerConnectionString += hostAndPortToUrlString(KAFKA_HOST, kafkaServer.socketServer().boundPort(listenerName));
            brokerConnectionString +=  ",";
        }

        LOG.info("ZK and KafkaServer started.");

        standardProps = new Properties();
        standardProps.setProperty("zookeeper.connect", zookeeperConnectionString);
        standardProps.setProperty("bootstrap.servers", brokerConnectionString);
        standardProps.setProperty("group.id", "flink-tests");
        standardProps.setProperty("enable.auto.commit", "false");
        standardProps.setProperty("zookeeper.session.timeout.ms", String.valueOf(zkTimeout));
        standardProps.setProperty("zookeeper.connection.timeout.ms", String.valueOf(zkTimeout));
        standardProps.setProperty("auto.offset.reset", "earliest"); // read from the beginning.
        standardProps.setProperty("max.partition.fetch.bytes", "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)

    }

    protected KafkaServer getKafkaServer(int brokerId, File tmpFolder) throws Exception {
        Properties kafkaProperties = new Properties();

        // properties have to be Strings
        kafkaProperties.put("advertised.host.name", KAFKA_HOST);
        kafkaProperties.put("broker.id", Integer.toString(brokerId));
        kafkaProperties.put("log.dir", tmpFolder.toString());
        kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
        kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024));
        kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));
        kafkaProperties.put("transaction.max.timeout.ms", Integer.toString(1000 * 60 * 60 * 2)); // 2hours

        // for CI stability, increase zookeeper session timeout
        kafkaProperties.put("zookeeper.session.timeout.ms", zkTimeout);
        kafkaProperties.put("zookeeper.connection.timeout.ms", zkTimeout);

        if (config.getKafkaServerProperties() != null) {
            kafkaProperties.putAll(config.getKafkaServerProperties());
        }

        final int numTries = 5;

        for (int i = 1; i <= numTries; i++) {
            int kafkaPort = NetUtils.getAvailablePort();
            kafkaProperties.put("port", Integer.toString(kafkaPort));

//            if (config.isHideKafkaBehindProxy()) {
//                NetworkFailuresProxy proxy = createProxy(KAFKA_HOST, kafkaPort);
//                kafkaProperties.put("advertised.port", proxy.getLocalPort());
//            }

            KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

            try {
                scala.Option<String> stringNone = scala.Option.apply(null);
                KafkaServer server = new KafkaServer(kafkaConfig, Time.SYSTEM, stringNone, new ArraySeq<KafkaMetricsReporter>(0));
                server.startup();
                return server;
            }
            catch (KafkaException e) {
                if (e.getCause() instanceof BindException) {
                    // port conflict, retry...
                    LOG.info("Port conflict when starting Kafka Broker. Retrying...");
                }
                else {
                    throw e;
                }
            }
        }

        throw new Exception("Could not start Kafka after " + numTries + " retries due to port conflicts.");
    }

    public void shutdown() throws Exception {
        for (KafkaServer broker : brokers) {
            if (broker != null) {
                broker.shutdown();
            }
        }
        brokers.clear();

        if (zookeeper != null) {
            try {
                zookeeper.stop();
            }
            catch (Exception e) {
                LOG.warn("ZK.stop() failed", e);
            }
            zookeeper = null;
        }

        // clean up the temp spaces

        if (tmpKafkaParent != null && tmpKafkaParent.exists()) {
            try {
                FileUtils.deleteDirectory(tmpKafkaParent);
            }
            catch (Exception e) {
                // ignore
            }
        }
        if (tmpZkDir != null && tmpZkDir.exists()) {
            try {
                FileUtils.deleteDirectory(tmpZkDir);
            }
            catch (Exception e) {
                // ignore
            }
        }
    }

    public void deleteTestTopic(String topic) {
        LOG.info("Deleting topic {}", topic);
        Properties props = new Properties();
        props.putAll(getStandardProperties());
        String clientId = Long.toString(new Random().nextLong());
        props.put("client.id", clientId);
        AdminClient adminClient = AdminClient.create(props);
        // We do not use a try-catch clause here so we can apply a timeout to the admin client closure.
        try {
            tryDelete(adminClient, topic);
        } catch (Exception e) {
            e.printStackTrace();
            fail(String.format("Delete test topic : %s failed, %s", topic, e.getMessage()));
        } finally {
            adminClient.close(Duration.ofMillis(5000L));
            maybePrintDanglingThreadStacktrace(clientId);
        }
    }

    protected void maybePrintDanglingThreadStacktrace(String threadNameKeyword) {
        for (Map.Entry<Thread, StackTraceElement[]> threadEntry : Thread.getAllStackTraces().entrySet()) {
            if (threadEntry.getKey().getName().contains(threadNameKeyword)) {
                System.out.println("Dangling thread found:");
                for (StackTraceElement ste : threadEntry.getValue()) {
                    System.out.println(ste);
                }
            }
        }
    }

    private void tryDelete(AdminClient adminClient, String topic)
        throws Exception {
        try {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get(DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.info("Did not receive delete topic response within {} seconds. Checking if it succeeded",
                DELETE_TIMEOUT_SECONDS);
            if (adminClient.listTopics().names().get(DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS).contains(topic)) {
                throw new Exception("Topic still exists after timeout");
            }
        }
    }
    public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
        this.createTestTopic(topic, numberOfPartitions, replicationFactor, new Properties());
    }

    public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor, Properties properties) {
        LOG.info("Creating topic {}", topic);
        try (AdminClient adminClient = AdminClient.create(getStandardProperties())) {
            NewTopic topicObj = new NewTopic(topic, numberOfPartitions, (short) replicationFactor);
            adminClient.createTopics(Collections.singleton(topicObj)).all().get();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Create test topic : " + topic + " failed, " + e.getMessage());
        }
    }

    public static Config createConfig() {
        return new Config();
    }

    public static class Config {
        private int kafkaServersNumber = 1;
        private Properties kafkaServerProperties = null;
        private boolean secureMode = false;
        private boolean hideKafkaBehindProxy = false;

        /**
         * Please use {@link KafkaTestEnv#createConfig()} method.
         */
        private Config() {
        }

        public int getKafkaServersNumber() {
            return kafkaServersNumber;
        }

        public Config setKafkaServersNumber(int kafkaServersNumber) {
            this.kafkaServersNumber = kafkaServersNumber;
            return this;
        }

        public Properties getKafkaServerProperties() {
            return kafkaServerProperties;
        }

        public Config setKafkaServerProperties(Properties kafkaServerProperties) {
            this.kafkaServerProperties = kafkaServerProperties;
            return this;
        }
    }
}

package hua.mulan.slink;

import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/28
 **/
public class KafkaTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);

    protected static KafkaTestEnv kafkaServer;
    protected static String brokerConnectionStrings;
    protected static Properties standardProps;

    @BeforeClass
    public static void prepare() throws Exception {
        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Starting KafkaTestBase ");
        LOG.info("-------------------------------------------------------------------------");
        startClusters(KafkaTestEnv.createConfig());
    }

    @AfterClass
    public static void shutDownServices() throws Exception {

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Shut down KafkaTestBase ");
        LOG.info("-------------------------------------------------------------------------");

        TestStreamEnvironment.unsetAsContext();

        shutdownClusters();

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    KafkaTestBase finished");
        LOG.info("-------------------------------------------------------------------------");
    }

    protected static void startClusters(KafkaTestEnv.Config environmentConfig) throws Exception {
        kafkaServer = new KafkaTestEnv();

        LOG.info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.getVersion());

        kafkaServer.prepare(environmentConfig);
        standardProps = kafkaServer.getStandardProperties();
        brokerConnectionStrings = kafkaServer.getBrokerConnectionString();
    }

    protected static void shutdownClusters() throws Exception {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
    }

    protected static void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
        kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor);
    }

    protected static void deleteTestTopic(String topic) {
        kafkaServer.deleteTestTopic(topic);
    }

}

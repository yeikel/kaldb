package com.slack.kaldb.logstore.search;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.Kaldb;
import com.slack.kaldb.server.KaldbIndexer;
import com.slack.kaldb.server.KaldbLocalSearcher;
import com.slack.kaldb.server.QueryService;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static com.slack.kaldb.testlib.KaldbConfigUtil.makeKaldbConfig;
import static org.assertj.core.api.Assertions.assertThat;

public class QueryServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static SimpleMeterRegistry metricsRegistry;
  private static ChunkManagerUtil<LogMessage> chunkManagerUtil;

  private static KaldbServiceGrpc.KaldbServiceBlockingStub indexerServiceStub;
  private static Server server;

  private static final String TEST_KAFKA_TOPIC = "test-topic";

  // Kafka producer creates only a partition 0 on first message. So, set the partition to 0 always.
  private static final int TEST_KAFKA_PARTITION = 0;

  private static final String KALDB_TEST_CLIENT = "kaldb-test-client";
  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static TestKafkaServer kafkaServer;


  @BeforeClass
  public static void initialize() throws Exception {
    kafkaServer = new TestKafkaServer();
    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();

    KaldbConfigs.KaldbConfig kaldbConfig = makeKaldbConfig("localhost:" + broker.getKafkaPort().get(), 8081, TEST_KAFKA_TOPIC, TEST_KAFKA_PARTITION, KALDB_TEST_CLIENT, TEST_S3_BUCKET);
    KaldbConfig.initFromConfigObject(kaldbConfig);

    Kaldb kaldb = new Kaldb(kaldbConfig);
    kaldb.setup();

//    server = newIndexingServer(kaldbConfig);
//    server.start().join();
    indexerServiceStub =
        Clients.newClient(
            "gproto+http://127.0.0.1:8081/", KaldbServiceGrpc.KaldbServiceBlockingStub.class);
  }

  private static Server newIndexingServer(KaldbConfigs.KaldbConfig kaldbConfig) {
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
    KaldbIndexer indexer =
        new KaldbIndexer(
            chunkManagerUtil.chunkManager, KaldbIndexer.dataTransformerMap.get("api_log"), metricsRegistry);
    indexer.start();

    KaldbLocalSearcher<LogMessage> service = new KaldbLocalSearcher<>(indexer.getChunkManager());

    return Server.builder()
        .http(kaldbConfig.getServerPort())
        .verboseResponses(true)
        .service(GrpcService.builder().addService(service).build())
        .build();
  }

  @AfterClass
  public static void shutdownServer() throws Exception {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    metricsRegistry.close();
    if (server != null) {
      server.stop().join();
    }
  }

  @Test
  public void testSearch() {
    indexerServiceStub.search(
        KaldbSearch.SearchRequest.newBuilder()
            .setIndexName(MessageUtil.TEST_INDEX_NAME)
            .setQueryString("queryString")
            .setStartTimeEpochMs(0)
            .setEndTimeEpochMs(1)
            .setHowMany(10)
            .setBucketCount(2)
            .build());
  }
}

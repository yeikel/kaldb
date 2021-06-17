package com.slack.kaldb.server;

import com.google.common.annotations.VisibleForTesting;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.grpc.GrpcMeterIdPrefixFunction;
import com.linecorp.armeria.common.logging.LogLevel;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.logging.LoggingService;
import com.linecorp.armeria.server.logging.LoggingServiceBuilder;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of Kaldb that sets up the basic infra needed for all the other end points like an a
 * http server, register monitoring libraries, create config manager etc..
 */
public class Kaldb {
  private static final Logger LOG = LoggerFactory.getLogger(Kaldb.class);

  private final MeterRegistry meterRegistry;

  public static String READ_NODE_ROLE = "read";
  public static String CACHE_NODE_ROLE = "cache";
  public static String INDEX_NODE_ROLE = "index";

  public Kaldb(Path configFilePath) throws IOException {
    this.meterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Metrics.addRegistry(meterRegistry);

    KaldbConfig.initFromFile(configFilePath);
  }

  @VisibleForTesting
  public Kaldb(KaldbConfigs.KaldbConfig kaldbConfig) {
    this.meterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Metrics.addRegistry(meterRegistry);

    KaldbConfig.initFromConfigObject(kaldbConfig);
  }

  public void setup() {
    LOG.info("Starting Kaldb server");

    final int serverPort = KaldbConfig.get().getServerPort();
    // Create an API server to serve the search requests.
    ServerBuilder sb = Server.builder();
    sb.decorator(
        MetricCollectingService.newDecorator(GrpcMeterIdPrefixFunction.of("grpc.service")));
    sb.decorator(getLoggingServiceBuilder().newDecorator());
    sb.http(serverPort);
    sb.http2MaxStreamsPerConnection()
    sb.service("/health", HealthCheckService.builder().build());
    if (meterRegistry instanceof PrometheusMeterRegistry) {
      setupMetrics();
      sb.service("/metrics", (ctx, req) -> HttpResponse.of(((PrometheusMeterRegistry) meterRegistry).scrape()));
    }
    sb.serviceUnder("/docs", new DocService());

    String nodeRole = KaldbConfig.get().getNodeRole();
    GrpcServiceBuilder searchBuilder;
    if (nodeRole.equals(READ_NODE_ROLE)) {
      searchBuilder = GrpcService.builder()
          .addService(new QueryService<>())
          .enableUnframedRequests(true);
    } else if ((nodeRole.equals(INDEX_NODE_ROLE))) {
      // Create an indexer and a grpc search service.
      KaldbIndexer indexer = KaldbIndexer.fromConfig(meterRegistry);

      // Create a protobuf handler service that calls chunkManager on search.
      searchBuilder = GrpcService.builder()
          .addService(new KaldbLocalSearcher<>(indexer.getChunkManager()))
          .enableUnframedRequests(true);

      // TODO: Instead of passing in the indexer, consider creating an interface or make indexer of
      // subclass of this class?
      // TODO: Start in a background thread.
      indexer.start();
    } else {
      throw new RuntimeException("The config must specify a nodeRole");
    }
    sb.service(searchBuilder.build());

    Server server = sb.build();
    CompletableFuture<Void> serverFuture = server.start();
    serverFuture.join();
    LOG.info("Started server on port: {}", serverPort);

    // TODO: On CTRL-C shut down the process cleanly. Ensure no write locks in indexer. Guava
    // ServiceManager?
  }

  private LoggingServiceBuilder getLoggingServiceBuilder() {
    return LoggingService.builder()
        // Not logging any successful response, say prom scraping /metrics every 30 seconds at INFO
        .successfulResponseLogLevel(LogLevel.DEBUG)
        .failureResponseLogLevel(LogLevel.ERROR)
        // Remove all headers to be sure we aren't leaking any auth/cookie info
        .requestHeadersSanitizer((ctx, headers) -> DefaultHttpHeaders.EMPTY_HEADERS);
  }

  private void setupMetrics() {
    // Expose JVM metrics.
    new ClassLoaderMetrics().bindTo(meterRegistry);
    new JvmMemoryMetrics().bindTo(meterRegistry);
    new JvmGcMetrics().bindTo(meterRegistry);
    new ProcessorMetrics().bindTo(meterRegistry);
    new JvmThreadMetrics().bindTo(meterRegistry);
    LOG.info("Done registering standard JVM metrics.");
  }

  public void close() {
    LOG.info("Shutting down Kaldb server");
    // TODO: Add a on exit method handler for the serve?
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      LOG.info("Config file is needed a first argument");
    }
    Path configFilePath = Path.of(args[0]);

    Kaldb kalDb = new Kaldb(configFilePath);
    kalDb.setup();
  }
}

package com.slack.kaldb.server;

import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.RequestContext;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QueryService<T> extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(QueryService.class);

  public static String servers;

  public KaldbSearch.SearchResult doSearch(KaldbSearch.SearchRequest request) {
    KaldbServiceGrpc.KaldbServiceBlockingStub stub =
        Clients.newClient(
            servers, KaldbServiceGrpc.KaldbServiceBlockingStub.class);
    KaldbSearch.SearchResult result = stub.search(request);
    return result;
  }

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    KaldbSearch.SearchResult protoSearchResult = doSearch(request);
    responseObserver.onNext(protoSearchResult);
    responseObserver.onCompleted();
  }
}

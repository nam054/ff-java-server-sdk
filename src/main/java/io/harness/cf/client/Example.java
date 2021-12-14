package io.harness.cf.client;

import io.harness.cf.client.api.CfClient;
import io.harness.cf.client.dto.Target;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class Example {

  public static final String FEATURE_FLAG_KEY = "toggle";
  public static final String API_KEY = "d400268d-f7aa-4bff-872d-ea334906eacd";
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public static void main(String... args) {
    CfClient cfClient = new CfClient(API_KEY);
    Target target =
        Target.builder()
            .identifier("target1")
            .isPrivate(false)
            .attribute("testKey", "TestValue")
            .name("target1")
            .build();

    scheduler.scheduleAtFixedRate(
        () -> {
          final boolean bResult = cfClient.boolVariation("flag1", target, false);
          log.info("Boolean variation: {}", bResult);
        },
        0,
        10,
        TimeUnit.SECONDS);
  }
}

package io.harness.cf.client.example;

import com.google.gson.JsonObject;
import io.harness.cf.client.api.Client;
import io.harness.cf.client.api.Config;
import io.harness.cf.client.api.FileMapStore;
import io.harness.cf.client.dto.Target;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class Example {

  private static final int capacity;
  private static final HashMap<String, String> keys;
  private static final ScheduledExecutorService scheduler;

  private static final String FREEMIUM_API_KEY = "45d2a13a-c62f-4116-a1a7-86f25d715a2e";
  private static final String NON_FREEMIUM_API_KEY = "9ecc4ced-afc1-45af-9b54-c899cbff4b62";
  private static final String NON_FREEMIUM_API_KEY_2 = "33e0b6ca-67f0-4af9-921d-945b89e26a3c";

  static {
    capacity = 5;
    keys = new HashMap<>(capacity);
    keys.put("Freemium", FREEMIUM_API_KEY);
    keys.put("Non-Freemium", NON_FREEMIUM_API_KEY);
    keys.put("Non-Freemium-2", NON_FREEMIUM_API_KEY_2);
    scheduler = Executors.newScheduledThreadPool(keys.size());
  }

  public static void main(String... args) {

    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdown));

    for (final String keyName : keys.keySet()) {

      final String apiKey = keys.get(keyName);
      final FileMapStore fileStore = new FileMapStore(keyName);
      final Client client = new Client(apiKey, Config.builder().store(fileStore).build());
      final String logPrefix = keyName + " :: " + client.hashCode() + " ";

      Target target =
          Target.builder()
              .identifier("target1")
              .isPrivate(false)
              .attribute("testKey", "TestValue")
              .name("target1")
              .build();

      scheduler.scheduleAtFixedRate(
          () -> {
            final boolean bResult = client.boolVariation("flag1", target, false);
            log.info(logPrefix + "Boolean variation: {}", bResult);

            final double dResult = client.numberVariation("flag2", target, -1);
            log.info(logPrefix + "Number variation: {}", dResult);

            final String sResult = client.stringVariation("flag3", target, "NO_VALUE!!!");
            log.info(logPrefix + "String variation: {}", sResult);

            final JsonObject jResult = client.jsonVariation("flag4", target, new JsonObject());
            log.info(logPrefix + "JSON variation: {}", jResult);
          },
          0,
          10,
          TimeUnit.SECONDS);
    }

    Thread.yield();
  }
}

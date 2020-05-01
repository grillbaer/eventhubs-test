package grillbaer;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Test app sending simple text events to an AzureEvent hub without
 * partition assignment, i.e. in a round robin manner.
 */
public final class RoundRobinSenderMain {
    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinSenderMain.class);

    public static void main(String[] args) throws InterruptedException, IOException {

        final Credentials creds = Credentials.readFromDefault();
        final EventHubProducerClient producer = new EventHubClientBuilder().
                connectionString(creds.getEventHubConnectionString(), creds.getEventHubName()).
                buildProducerClient();

        Runtime.getRuntime().addShutdownHook(new Thread("EH-Producer-ShutdownHook") {
            @Override
            public void run() {
                LOG.info("Stopping producer ...");
                producer.close();
                LOG.info("Stopped producer");
            }
        });

        for (int i = 1; i < 1000000; i++) {
            final String eventText = "Servus Bayern #" + i;
            final EventDataBatch batch = producer.createBatch();
            batch.tryAdd(new EventData(eventText));
            LOG.info("Sending event '{}' ...", eventText);
            producer.send(batch);

            Thread.sleep(500);
        }
    }
}

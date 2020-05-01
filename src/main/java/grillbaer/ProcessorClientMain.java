package grillbaer;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Test app consuming events from a EventHub using Azure SDK's {@linkplain EventProcessorClient} for
 * auto-balancing, failover and more. Simply outputs all received event texts to log.
 */
public final class ProcessorClientMain {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessorClientMain.class);

    public static void main(String[] args) throws IOException {
        final Credentials creds = Credentials.readFromDefault();

        final Consumer<EventContext> processEvent = eventContext -> {
            LOG.info("Received event '" + eventContext.getEventData().getBodyAsString()
                    + "' from partition " + eventContext.getPartitionContext().getPartitionId());
            eventContext.updateCheckpoint();
        };

        final Consumer<ErrorContext> processError = errorContext -> LOG.info("Received error", errorContext.getThrowable());

        final BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
                .connectionString(creds.getBlobConnectionString())
                .containerName(creds.getBlobContainerName())
                .sasToken(creds.getBlobKey())
                .buildAsyncClient();

        final EventProcessorClient processorClient = new EventProcessorClientBuilder()
                .connectionString(creds.getEventHubConnectionString(), creds.getEventHubName())
                .processEvent(processEvent)
                .processError(processError)
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
                .buildEventProcessorClient();

        Runtime.getRuntime().addShutdownHook(new Thread("EH-ProcessingClient-ShutdownHook") {
            @Override
            public void run() {
                LOG.info("Stopping event processor ... ");
                processorClient.stop();
                LOG.info("Event processor is stopped");
            }
        });

        LOG.info("Starting event processor ... ");
        processorClient.start();
        LOG.info("Event processor is running");
    }
}

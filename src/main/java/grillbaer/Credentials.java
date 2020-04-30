package grillbaer;

import lombok.Getter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * Overall credentials for accessing the Azure EventHub and also the BlobStorage for the checkpoint store.
 */
@Getter
final class Credentials {
    private final String eventHubConnectionString;
    private final String eventHubName;

    private final String blobConnectionString;
    private final String blobKey;
    private final String blobContainerName;

    public Credentials(String eventHubConnectionString, String eventHubName, String blobConnectionString, String blobKey, String blobContainerName) {
        this.eventHubConnectionString = Objects.requireNonNull(eventHubConnectionString);
        this.eventHubName = Objects.requireNonNull(eventHubName);
        this.blobConnectionString = Objects.requireNonNull(blobConnectionString);
        this.blobKey = Objects.requireNonNull(blobKey);
        this.blobContainerName = Objects.requireNonNull(blobContainerName);
    }

    public static Credentials readFromProperties(String s) throws IOException {
        final Properties props = new Properties();
        try (InputStream in = new FileInputStream(s)) {
            props.load(in);
        }

        return new Credentials(
                props.getProperty("eventHubConnectionString"),
                props.getProperty("eventHubName"),
                props.getProperty("blobConnectionString"),
                props.getProperty("blobKey"),
                props.getProperty("blobContainerName"));
    }

    public static Credentials readDefault() throws IOException {
        return Credentials.readFromProperties("secret-credentials.properties");
    }
}

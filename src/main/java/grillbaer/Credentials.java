package grillbaer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Overall credentials for accessing the Azure EventHub and also Azure BlobStorage for the checkpoint store.
 */
@Getter
@AllArgsConstructor
final class Credentials {
    private final @NonNull String eventHubConnectionString;
    private final @NonNull String eventHubName;

    private final @NonNull String blobConnectionString;
    private final @NonNull String blobKey;
    private final @NonNull String blobContainerName;

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

    public static Credentials readFromDefault() throws IOException {
        return Credentials.readFromProperties("secret-credentials.properties");
    }

    @Override
    public String toString() {
        return "Credentials[<secret content>]";
    }
}

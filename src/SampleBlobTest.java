import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.common.implementation.Constants;
import org.apache.commons.io.FileUtils;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;

public class SampleBlobTest extends TestHelper {

    static final String CONTAINER = "bigfiles";
    static final String BLOB = "MyBlob";
    static final int FILE_SIZE = 1000 * Constants.MB;
    static long BLOCK_SIZE = 5 * Constants.MB;
    static int MAX_CONCURRENCY = 40;
    static long MAX_SINGLE_UPLOAD_SIZE = 10 * Constants.MB;

    public static void main(String[] args) throws IOException {

        BlockBlobClient syncClient = new SpecializedBlobClientBuilder()
                .endpoint(ENDPOINT)
                .sasToken(SAS)
                .containerName(CONTAINER)
                .blobName(BLOB)
                .buildBlockBlobClient();

        uploadSyncBlockBlobClient(syncClient);

//        BlobAsyncClient asyncClient = new BlobClientBuilder()
//                .endpoint(ENDPOINT)
//                .sasToken(SAS)
//                .containerName(CONTAINER)
//                .blobName(BLOB)
//                .buildAsyncClient();

//        uploadAsyncBlockBlobClient(asyncClient);

    }
    
    private static void uploadSyncBlockBlobClient(BlockBlobClient client) throws IOException {
        File tempFile1 = DataGenerator.createTempLocalFile("blockblob", ".tmp", FILE_SIZE);

        long BLOCK_SIZE = 5 * Constants.MB;
        int MAX_CONCURRENCY = 40;
        long MAX_SINGLE_UPLOAD_SIZE = 10 * Constants.MB;

        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                .setBlockSizeLong(BLOCK_SIZE)
                .setMaxConcurrency(MAX_CONCURRENCY)
                .setMaxSingleUploadSizeLong(MAX_SINGLE_UPLOAD_SIZE);

        BlobOutputStream blobOutputStream = client.getBlobOutputStream(parallelTransferOptions, null, null, AccessTier.HOT, null);
        blobOutputStream.write(FileUtils.readFileToByteArray(tempFile1));
        blobOutputStream.close();

        System.out.println("Upload is done");
    }

    private static void uploadAsyncBlockBlobClient(BlobAsyncClient client) throws IOException {
        File tempFile1 = DataGenerator.createTempLocalFile("blockblob", ".tmp", FILE_SIZE);
        Flux<ByteBuffer> data = Flux.just(ByteBuffer.wrap(FileUtils.readFileToByteArray(tempFile1)));

        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                .setBlockSizeLong(BLOCK_SIZE)
                .setMaxConcurrency(MAX_CONCURRENCY)
                .setMaxSingleUploadSizeLong(MAX_SINGLE_UPLOAD_SIZE);

        client.upload(data, parallelTransferOptions, true).block(Duration.ofSeconds(300));

        System.out.println("Upload is done");
    }

}

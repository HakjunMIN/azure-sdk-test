import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
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
    static final int FILE_SIZE = 300 * Constants.MB;
    static long BLOCK_SIZE = 5 * Constants.MB;
    static int MAX_CONCURRENCY = 4;
    static long MAX_SINGLE_UPLOAD_SIZE = 10 * Constants.MB;

    public static void main(String[] args) throws IOException {

//        System.setProperty("reactor.bufferSize.small", "8");
//        System.setProperty("reactor.bufferSize.x", "8");
//        System.setProperty("reactor.netty.ioWorkerCount", "4");

        File tempFile1 = DataGenerator.createTempLocalFile("blockblob", ".tmp", FILE_SIZE);

        BlockBlobClient syncClient = new SpecializedBlobClientBuilder()
                .endpoint(ENDPOINT)
                .sasToken(SAS)
                .containerName(CONTAINER)
                .blobName(BLOB)
                .buildBlockBlobClient();

        uploadSyncSpecializedBlockBlobClient(syncClient, tempFile1);

        BlobAsyncClient asyncClient = new BlobClientBuilder()
                .endpoint(ENDPOINT)
                .sasToken(SAS)
                .containerName(CONTAINER)
                .blobName(BLOB)
                .buildAsyncClient();

        uploadBlobAsyncClient(asyncClient, tempFile1);

        BlobClient blobGeneralClient = new BlobClientBuilder()
                .endpoint(ENDPOINT)
                .sasToken(SAS)
                .containerName(CONTAINER)
                .blobName(BLOB)
                .buildClient();

        uploadSyncGeneralBlobClient(blobGeneralClient, tempFile1);
    }
    
    private static void uploadSyncSpecializedBlockBlobClient(BlockBlobClient client, File file) throws IOException {

        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                .setBlockSizeLong(BLOCK_SIZE)
                .setMaxConcurrency(MAX_CONCURRENCY)
                .setMaxSingleUploadSizeLong(MAX_SINGLE_UPLOAD_SIZE);

        BlobOutputStream blobOutputStream = client.getBlobOutputStream(parallelTransferOptions, null, null, AccessTier.HOT, null);
        blobOutputStream.write(FileUtils.readFileToByteArray(file));
        blobOutputStream.close();

        System.out.println("Upload using BlockBlobClient is done");
    }

    private static void uploadBlobAsyncClient(BlobAsyncClient client, File file) throws IOException {

        Flux<ByteBuffer> data = Flux.just(ByteBuffer.wrap(FileUtils.readFileToByteArray(file)));

        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                .setBlockSizeLong(BLOCK_SIZE)
                .setMaxConcurrency(MAX_CONCURRENCY)
                .setMaxSingleUploadSizeLong(MAX_SINGLE_UPLOAD_SIZE);

        client.upload(data, parallelTransferOptions, true).block(Duration.ofSeconds(300));

        System.out.println("Upload using BlobAsyncClient done");
    }


    private static void uploadSyncGeneralBlobClient(BlobClient client, File file) throws IOException {

        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                .setBlockSizeLong(BLOCK_SIZE)
                .setMaxConcurrency(MAX_CONCURRENCY)
                .setMaxSingleUploadSizeLong(MAX_SINGLE_UPLOAD_SIZE);

        client.uploadFromFile(file.getAbsolutePath(), parallelTransferOptions, null, null, AccessTier.HOT, null, null);
//        client.upload(BinaryData.fromBytes((FileUtils.readFileToByteArray(file))), true);
        System.out.println("Upload using BlobClient is done");
    }

}

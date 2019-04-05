package gcp.cm.mlapi.speech;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StorageSupport {
    private String bucket;
    private String folder;
    private String extension;
    private Storage storage;

    public StorageSupport(String bucket, String folder, String extension) {
        this.bucket = bucket;
        this.folder = folder;
        this.extension = extension;
        this.storage = StorageOptions.getDefaultInstance().getService();
    }

    public boolean fileExists(String name) {
        Blob blob = storage.get(bucket, name, Storage.BlobGetOption.fields(Storage.BlobField.NAME));
        return blob != null;
    }

    public List<String> listFilesToProcess(String suffix) {
        Page<Blob> blobs = storage.list(bucket, Storage.BlobListOption.prefix(folder));
        List<String> results = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (blob.getName().endsWith(suffix)) {
                if (!fileExists(blob.getName() + "." + extension)) {
                    results.add(blob.getName());
                }
            }
        }
        return results;
    }

    public void store(String name, String contents) {
        BlobId blobId = BlobId.of(bucket, name + "." + extension);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        Blob blob = storage.create(blobInfo, contents.getBytes(UTF_8));
    }

    public String getBucket() {
        return bucket;
    }


    public String getFolder() {
        return folder;
    }
}

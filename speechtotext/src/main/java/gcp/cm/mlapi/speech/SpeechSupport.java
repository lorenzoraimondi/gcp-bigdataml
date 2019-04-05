package gcp.cm.mlapi.speech;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.speech.v1p1beta1.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class SpeechSupport implements AutoCloseable {
    private RecognitionConfig config;
    SpeechClient speechClient;

    public SpeechSupport(RecognitionConfig config) {
        this.config = config;
        try {
            speechClient = SpeechClient.create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String recognize(RecognitionAudio audio) {
        try {
            OperationFuture<LongRunningRecognizeResponse, LongRunningRecognizeMetadata> operation;
            operation = speechClient.longRunningRecognizeAsync(config, audio);
            while (!operation.isDone()) {
                System.out.print(">");
                Thread.sleep(6000); // 10 ticks make 1 minute
            }
            System.out.println("|");
            LongRunningRecognizeResponse response = operation.get();
            StringBuffer buffer = new StringBuffer();
            for (SpeechRecognitionResult result : response.getResultsList()) {
                buffer.append(result.getAlternatives(0).getTranscript().trim());
                buffer.append("\n");
            }
            return buffer.toString();
        } catch (PermissionDeniedException e) {
            System.err.println("Access denied to GCP resource : " + e.getMessage());
            return null;
        } catch (InvalidArgumentException e) {
            System.err.println("Invalid argument to GCP API : " + e.getMessage());
            return null;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {
        speechClient.close();
    }

}

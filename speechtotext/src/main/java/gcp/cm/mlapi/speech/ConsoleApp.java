package gcp.cm.mlapi.speech;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.cloud.speech.v1p1beta1.*;
import com.google.cloud.storage.StorageException;

import java.util.List;

public class ConsoleApp {

    @Parameter(names={"--bucket", "-b"}, description = "The bucket to read from and write to")
    private String bucket = "gcp-bigdataml";
    @Parameter(names={"--folder", "-f"}, description = "The folder to read from and write to")
    private String folder = "audio";
    @Parameter(names={"--extension", "--ext", "-x"}, description = "The audio extension for the files to convert")
    private String suffix = "wav";
    @Parameter(names={"--outputExtension", "--out", "-o"}, description = "The extension to attache to the script files")
    private String extension = "txt";
    @Parameter(names={"--encoding", "--enc", "-c"}, description = "The audio encoding of the files to convert")
    private RecognitionConfig.AudioEncoding encoding = RecognitionConfig.AudioEncoding.LINEAR16;
    @Parameter(names={"--sampleRate", "--rate", "-r"}, description = "The audio sampling rate of the files to convert in Hertz")
    private int sampleRateHertz = 16000;
    @Parameter(names={"--language", "--lang", "-l"}, description = "The language of the speech")
    private String languageCode = "it-IT";
    @Parameter(names = {"--help", "-h"}, help = true, description = "Show this help screen")
    private boolean isHelp = false;

    public static void main(String[] args) {
        ConsoleApp app = new ConsoleApp();
        JCommander jcmd = new JCommander(app);
        jcmd.setProgramName("speech-to-text");
        jcmd.parse(args);
        if (app.isHelp) {
            jcmd.usage();
        } else {
            app.run();
        }
    }

    private void run() {
        // Prepare the client for Cloud Storage
        StorageSupport storage = new StorageSupport(bucket, folder, extension);
        // Get the list of files to be processed
        List<String> files = storage.listFilesToProcess(suffix);
        // Prepare the configuration for Cloud Speech API
        RecognitionConfig config = RecognitionConfig.newBuilder()
                .setEncoding(encoding)
                .setSampleRateHertz(sampleRateHertz)
                .setLanguageCode(languageCode)
                .setEnableAutomaticPunctuation(true)
                .build();
        // Process the files
        if (!files.isEmpty()) {
            try (SpeechSupport recognition = new SpeechSupport(config)) {
                for (String name : files) {
                    // Prepare the audio information
                    String uri = String.format("gs://%s/%s", storage.getBucket(), name);
                    RecognitionAudio audio = RecognitionAudio.newBuilder().setUri(uri).build();
                    // Get the script for the audio file
                    System.out.print("> Recognizing text for " + name + "  ");
                    String script = recognition.recognize(audio);
                    System.out.println("> Writing script for " + name);
                    try {
                        storage.store(name, script);
                    } catch (StorageException e) {
                        System.err.println("! Could not store " + name + " due to " + e.getMessage());
                    }
                }
            }
        } else {
            System.out.println("> No files to be processed");
        }
    }
}

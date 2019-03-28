package gcp.cm.bigdata.adtech.dataflow;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.json.JSONObject;

import java.io.*;
import java.util.Map;

public class JSONObjectCoder extends AtomicCoder<JSONObject> {

    @Override
    public void encode(JSONObject value, OutputStream outStream) throws CoderException, IOException {
        new ObjectOutputStream(outStream).writeObject(value.toMap());
    }

    @Override
    public JSONObject decode(InputStream inStream) throws CoderException, IOException {
        try {
            return new JSONObject((Map)new ObjectInputStream(inStream).readObject());
        } catch (ClassNotFoundException e) {
            throw new CoderException(e);
        }
    }
}

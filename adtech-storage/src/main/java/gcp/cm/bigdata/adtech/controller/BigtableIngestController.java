package gcp.cm.bigdata.adtech.controller;

import gcp.cm.bigdata.adtech.BigtableHelper;
import gcp.cm.bigdata.adtech.domain.Impression;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

import static com.googlecode.objectify.ObjectifyService.ofy;

@RestController
@RequestMapping("bigtable")
@CrossOrigin
public class BigtableIngestController {

    @RequestMapping(path = "impression", method = RequestMethod.POST)
    public void ingest(@RequestBody Impression entry) {
        BigtableHelper.insertNewImpression(entry);
    }

}
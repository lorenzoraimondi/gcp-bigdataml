package gcp.cm.bigdata.adtech.controller;

import gcp.cm.bigdata.adtech.domain.Impression;
import org.springframework.web.bind.annotation.*;

import static com.googlecode.objectify.ObjectifyService.ofy;

@RestController
@RequestMapping("datastore")
@CrossOrigin
public class DatastoreIngestController {

    @RequestMapping(path = "impression", method = RequestMethod.POST)
    public void ingest(@RequestBody Impression entry) {
        ofy().save().entity(entry);
    }

}
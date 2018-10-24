package com.mrdc.solr2solr;

import com.codahale.metrics.Meter;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author ludovic
 */
public class SolrDocumentCallback extends StreamingResponseCallback {

    private ArrayList<SolrInputDocument> docs;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private long numFound;
    private long count;
    private Meter readMeter;
    private ConcurrentLinkedQueue<SolrInputDocument> queue;

    public SolrDocumentCallback(ICallback indexingCallback, int batchSize) {
        docs = new ArrayList<>();
        count = 0;
        readMeter = Starter.metrics.meter("read-docs");
    }

    public SolrDocumentCallback(ConcurrentLinkedQueue queue) {
        docs = new ArrayList<>();
        count = 0;
        readMeter = Starter.metrics.meter("read-docs");
        this.queue = queue;
    }

    @Override
    public void streamSolrDocument(SolrDocument sd) {
        readMeter.mark();
//        if (docs.size() < batchSize) {
        SolrInputDocument doc = toSolrInputDocument(sd);
        doc.remove("_version_");
//            docs.add(doc);
//        } else {
//            this.indexingCallback.execute(docs);
//        }
        addToQueue(doc);
        this.count += 1;

    }

    private boolean addToQueue(SolrInputDocument doc) {
        boolean insertResult = false;
        if (this.queue != null) {
            insertResult = queue.offer(doc);
            while (insertResult = false) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {

                }
                insertResult = queue.offer(doc);
            }
        }
        return insertResult;
    }

    @Override
    public void streamDocListInfo(long numFound, long start, Float maxScore) {
        this.numFound = numFound;
    }

    private SolrInputDocument toSolrInputDocument(SolrDocument d) {
        SolrInputDocument doc = new SolrInputDocument();

        for (String name : d.getFieldNames()) {
            doc.addField(name, d.getFieldValue(name));
        }

        return doc;
    }

    public long getNumFound() {
        return numFound;
    }

    public void setNumFound(long numFound) {
        this.numFound = numFound;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

}

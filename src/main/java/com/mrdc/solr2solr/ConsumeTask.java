/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import com.codahale.metrics.Meter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class ConsumeTask implements Callable<Boolean> {

    private ConcurrentLinkedQueue queue;
    private Meter writeMeter;
    private String collection;
    private IndexClient targetSolr;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean doneReading = false;
    private int solrBatchSize;
    private long count;
    private boolean startedReading = false;

    public ConsumeTask(String collection, IndexClient targetSolr, ConcurrentLinkedQueue<SolrInputDocument> queue, int solrBatchSize) {
        this.queue = queue;
        this.writeMeter = Starter.metrics.meter("write-docs");
        this.collection = collection;
        this.targetSolr = targetSolr;
        this.solrBatchSize = solrBatchSize;
    }

    @Override
    public Boolean call() {
        logger.info("Start writing documents to Solr cluster {}", targetSolr.getZkHost()[0]);
        ArrayList<SolrInputDocument> docs = new ArrayList<>();
        boolean isWriting = true;

        while (isWriting == true) {
            //wait until the producer has started reading documents
            if (startedReading == false) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ex) {
                }
            }
            isWriting = (!queue.isEmpty() || doneReading == false);
//            if (isWriting == false) {
//                logger.info("ici: {} {} {} ", queue.size() > 0, doneReading, isWriting);
//            }
            if (!queue.isEmpty()) {
                SolrInputDocument solrDoc = (SolrInputDocument) queue.poll();
                docs.add(solrDoc);
                if (docs.size() == solrBatchSize) {
                    writeMeter.mark(solrBatchSize);
                    pushToSolr(docs);
                    count += docs.size();
                    docs.clear();
                }
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                }
            }

        }

        if (docs.size() > 0) {
            pushToSolr(docs);
            count += docs.size();
        }
        logger.info("Finish writing documents");
        return true;
    }

    public void setDoneReading(Boolean readStatus) {
        this.doneReading = readStatus;
    }

    public void setStartReading(boolean status) {
        this.startedReading = status;
    }

    private void pushToSolr(ArrayList<SolrInputDocument> docs) {
        try {
            targetSolr.indexDocuments(collection, docs);
        } catch (SolrServerException ex) {
            logger.error("SolrServerException: Failed to push documents", ex);
            doneReading = true;
        } catch (IOException ex) {
            logger.error("IOException: Failed to push documents", ex);
            doneReading = true;
        }
    }

}

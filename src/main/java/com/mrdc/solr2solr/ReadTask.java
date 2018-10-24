/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class ReadTask implements Callable<Boolean> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private ConcurrentLinkedQueue queue;
    private IndexClient solrClient;
    private String query;
    private String collection;
    private String[] fields;
    private ConsumeTask consumer;
    private int batchSize;

    public ReadTask(IndexClient solrClient, String collection, String[] fields, String query, ConcurrentLinkedQueue queue, ConsumeTask consumer, int readBatchSize) {
        this.queue = queue;
        this.consumer = consumer;
        this.solrClient = solrClient;
        this.fields = fields;
        this.query = query;
        this.collection = collection;
        this.batchSize = readBatchSize;
    }

    @Override
    public Boolean call() throws Exception {
        logger.info("Start Reading documents for query: {}", query);
        consumer.setDoneReading(false);
        SolrDocumentCallback solrCallback = new SolrDocumentCallback(queue);
        consumer.setStartReading(true);
        //add documents to the queue
        solrClient.queryWithStream(collection, query, fields, solrCallback, batchSize);
        consumer.setDoneReading(true);
        logger.info("Finish reading documents for query: {}", query);
        return true;
    }

}

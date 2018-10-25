/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.solr.client.solrj.SolrQuery;
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
    private String[] filterQuery;

    public ReadTask(IndexClient solrClient, String collection, String[] fields, String query, ConcurrentLinkedQueue queue, ConsumeTask consumer, int readBatchSize, String... filterQuery) {
        this.queue = queue;
        this.consumer = consumer;
        this.solrClient = solrClient;
        this.fields = fields;
        this.query = query;
        this.collection = collection;
        this.batchSize = readBatchSize;
        this.filterQuery = filterQuery;
    }

    @Override
    public Boolean call() throws Exception {
        consumer.setDoneReading(false);
        SolrDocumentCallback solrCallback = new SolrDocumentCallback(queue);
        consumer.setStartReading(true);
        //build solr query
        SolrQuery solrQuery = new SolrQuery(query);
        if (filterQuery != null && !filterQuery.equals("")) {
            solrQuery.setFilterQueries(filterQuery);
        }
        solrQuery.set("collection", collection);
        solrQuery.setSort("id", SolrQuery.ORDER.asc);

        solrQuery.setRequestHandler("/query");
        solrQuery.setRows(batchSize);
        logger.info("Start Reading documents for query: {}", solrQuery);
        //add documents to the queue
        solrClient.queryWithStream(collection, solrQuery, fields, solrCallback);
        consumer.setDoneReading(true);
        logger.info("Finish reading documents for query: {}", solrQuery);
        return true;
    }

}

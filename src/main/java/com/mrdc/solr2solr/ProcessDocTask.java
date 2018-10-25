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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class ProcessDocTask implements Callable<Boolean> {

    private IndexClient sourceSolr;
    private IndexClient targetSolr;
    private String query;
    private String sourceCollection;
    private String targetCollection;
    private String[] fields;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private int readBatchSize;
    private int writeBatchSize;

    public ProcessDocTask(IndexClient source, IndexClient target, String sourceCollection, String targetCollection, String query, String[] fields) {
        this(source, target, sourceCollection, targetCollection, query, fields, 20, 20);
    }
    
    public ProcessDocTask(IndexClient source, IndexClient target, String sourceCollection, String targetCollection, String query, String[] fields, int readBatchSize, int writeBatchSize) {
        this.sourceSolr = source;
        this.targetSolr = target;
        this.sourceCollection = sourceCollection;
        this.targetCollection = targetCollection;
        this.query = query;
        this.fields = fields;
        this.readBatchSize = readBatchSize;
        this.writeBatchSize = writeBatchSize;
    }

    @Override
    public Boolean call() throws Exception {

//        ICallback pushCallback = new ICallback() {
//            @Override
//            public void execute(SolrDocumentList solrDocs) {
//                try {
//                    targetSolr.indexDocuments(targetCollection, solrDocs);
//                } catch (SolrServerException ex) {
//                    logger.error("SolrServerException: Failed to push documents for account: {}", query, ex);
//                } catch (IOException ex) {
//                    logger.error("IOException: Failed to push documents for account: {}", query, ex);
//                }
//            };            
//
//            @Override
//            public void execute(ArrayList<SolrInputDocument> solrDocs) {
//                try {
//                    targetSolr.indexDocuments(targetCollection, solrDocs);
//                } catch (SolrServerException ex) {
//                    logger.error("SolrServerException: Failed to push documents for account: {}", query, ex);
//                } catch (IOException ex) {
//                    logger.error("IOException: Failed to push documents for account: {}", query, ex);
//                }
//            }
//
//        };        
        //sourceSolr.queryIndex(sourceCollection, query, pushCallback);
        //sourceSolr.queryWithStream(sourceCollection, query, fields, queue);
        ConcurrentLinkedQueue<SolrInputDocument> queue = new ConcurrentLinkedQueue();
        ConsumeTask indexDocsTask = new ConsumeTask(targetCollection, targetSolr, queue, writeBatchSize);
        FutureTask consumerFuture = new FutureTask(indexDocsTask);
        Thread t = new Thread(consumerFuture);
        t.start();
        ReadTask readSolrDocsTask = new ReadTask(sourceSolr, sourceCollection, fields, query, queue, indexDocsTask, readBatchSize);
        FutureTask producerFuture = new FutureTask(readSolrDocsTask);
        Thread t1 = new Thread(producerFuture);
        t1.start();

        //wait for the documents to be writen to Solr
        while(t1.isAlive()){            
            Thread.sleep(100);
        }

        return true;
    }

}

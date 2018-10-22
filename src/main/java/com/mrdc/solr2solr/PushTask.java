/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class PushTask implements Callable<Boolean> {

    private IndexClient sourceSolr;
    private IndexClient targetSolr;
    private String query;
    private String sourceCollection;
    private String targetCollection;
    private String[] fields;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public PushTask(IndexClient source, IndexClient target, String sourceCollection, String targetCollection, String query, String[] fields) {
        this.sourceSolr = source;
        this.targetSolr = target;
        this.sourceCollection = sourceCollection;
        this.targetCollection = targetCollection;
        this.query = query;
        this.fields = fields;
    }

    @Override
    public Boolean call() throws Exception {

        ICallback pushCallback = new ICallback() {
            @Override
            public void execute(SolrDocumentList solrDocs) {
                try {
                    targetSolr.indexDocuments(targetCollection, solrDocs);
                } catch (SolrServerException ex) {
                    logger.error("SolrServerException: Failed to push documents for account: {}", query, ex);
                } catch (IOException ex) {
                    logger.error("IOException: Failed to push documents for account: {}", query, ex);
                }
            };            

            @Override
            public void execute(ArrayList<SolrInputDocument> solrDocs) {
                try {
                    targetSolr.indexDocuments(targetCollection, solrDocs);
                } catch (SolrServerException ex) {
                    logger.error("SolrServerException: Failed to push documents for account: {}", query, ex);
                } catch (IOException ex) {
                    logger.error("IOException: Failed to push documents for account: {}", query, ex);
                }
            }

        };
        logger.info("Start processing account: {}", query);
        //sourceSolr.queryIndex(sourceCollection, query, pushCallback);
        sourceSolr.queryWithStream(sourceCollection, query, fields, pushCallback);
        logger.info("Finish processing account: {}", query);

        return true;
    }

}

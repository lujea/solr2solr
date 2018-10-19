/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class IndexClient {

    private String[] zkHost;
    private SolrClient cloudSolrClient;
    private int batchSize = 10;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public IndexClient() {
    }

    public IndexClient(String[] zkHost) {
        this.zkHost = zkHost;
        List<String> hosts = Arrays.asList(zkHost);
        cloudSolrClient = new CloudSolrClient.Builder().withZkHost(hosts).build();
    }

    public void queryIndex(String collection, String account) throws SolrServerException, IOException {
        queryIndex(collection, account, null);
    }

    public void queryIndex(String collection, String queryStr, ICallback callback) throws SolrServerException, IOException {        
        SolrQuery query = new SolrQuery(queryStr);
        query.set("collection", collection);
        query.setSort("id", SolrQuery.ORDER.asc);        

        query.set("cursorMark", "*");
        query.setRequestHandler("/query");
        query.setRows(batchSize);
        boolean isDone = false;

        int count = 0;
        while (isDone == false) {
            QueryResponse response = cloudSolrClient.query(query);
            count += response.getResults().size();
            long total = response.getResults().getNumFound();
            if ((count % 100 == 0) && count > 0) {
                logger.info("Processing query: {} ({}/{})", query, count, total);
            }
            String nextCursor = response.getNextCursorMark();
            if (callback != null) {
                callback.execute(response.getResults());
            }
            query.set("cursorMark", nextCursor);
        }
        logger.info("Finished reading documents for query: {}", query);

    }

    public void indexDocuments(String collection, SolrDocumentList docList) throws SolrServerException, IOException {

        List<SolrInputDocument> docs = new ArrayList<>();
        if (docList != null) {
            docList.parallelStream().forEach(doc -> {
                doc.removeFields("_version_");
                SolrInputDocument input = toSolrInputDocument(doc);

                if (input != null) {
                    docs.add(input);
                }
            });
            if (docs != null) {
                this.cloudSolrClient.add(collection, docs);
            }
        } else {
            logger.error("Failed pushing documents");
        }
    }

    public String[] getZkHost() {
        return zkHost;
    }

    public void setZkHost(String[] zkHost) {
        this.zkHost = zkHost;
    }

    private SolrInputDocument toSolrInputDocument(SolrDocument d) {
        SolrInputDocument doc = new SolrInputDocument();

        for (String name : d.getFieldNames()) {
            doc.addField(name, d.getFieldValue(name));
        }

        return doc;
    }

}

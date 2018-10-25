/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrdc.solr2solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ludovic
 */
public class IndexClient {

    private String[] zkHost;
    private SolrClient cloudSolrClient;
    private int batchSize = 15;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private StreamFactory streamFactory;
    private int numSolrClient = 10;
    private HashMap<Integer, SolrClient> solrClientMap = new HashMap<>();
    private Random random = new Random();

    public IndexClient() {
    }

    public IndexClient(String[] zkHost) {
        this.zkHost = zkHost;
        List<String> hosts = Arrays.asList(zkHost);
        cloudSolrClient = new CloudSolrClient.Builder().withZkHost(hosts).build();
    }

    public IndexClient(String[] zkHost, String collection) {
        this.zkHost = zkHost;
        List<String> hosts = Arrays.asList(zkHost);
        cloudSolrClient = new CloudSolrClient.Builder().withZkHost(hosts).build();
        streamFactory = new StreamFactory().withCollectionZkHost(collection, hosts.get(0));
//                .withStreamFunction("search", CloudSolrStream.class)
//                .withStreamFunction("unique", UniqueStream.class)
//                .withStreamFunction("top", RankStream.class)
//                .withStreamFunction("group", ReducerStream.class)
//                .withStreamFunction("parallel", ParallelStream.class);
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

    public void queryWithStream(String collection, String queryStr, String[] fields, ICallback callback) throws IOException, SolrServerException {
        SolrQuery query = new SolrQuery(queryStr);
        query.set("collection", collection);
        query.setSort("id", SolrQuery.ORDER.asc);

        query.set("cursorMark", "*");
        query.setRequestHandler("/query");
        query.setRows(batchSize);
        //query.set("shards", "10.10.40.183:31000/solr/ma_shard15_replica1");
        boolean isDone = false;
        SolrDocumentCallback solrCallback = new SolrDocumentCallback(callback, 10);
        long count = 0;
        while (isDone == false) {
            QueryResponse response = cloudSolrClient.queryAndStreamResponse(collection, query, solrCallback);
            count = solrCallback.getCount();
            long total = solrCallback.getNumFound();
            if ((count % 100 == 0) && count > 0) {
                logger.info("Processing query: ({}/{}) {}", count, total, query);
            }
            String nextCursor = response.getNextCursorMark();
            query.set("cursorMark", nextCursor);
        }
        logger.info("Finished reading documents for query: {}", query);
    }

    public boolean queryWithStream(String collection, String queryStr, String[] fields, SolrDocumentCallback solrCallback, int readBatchSize) throws IOException, SolrServerException {
        SolrQuery query = new SolrQuery(queryStr);
        query.set("collection", collection);
        query.setSort("id", SolrQuery.ORDER.asc);
        String cursorMark = "*";
        query.set("cursorMark", cursorMark);
        query.setRequestHandler("/query");
        query.setRows(readBatchSize);

        boolean isDone = false;
        long count = 0;
        long total = 0;
        while (isDone == false) {
            QueryResponse response = cloudSolrClient.queryAndStreamResponse(collection, query, solrCallback);
            String nextCursor = response.getNextCursorMark();
            count = solrCallback.getCount();
            total = solrCallback.getNumFound();
            if ((count % 100 == 0) && count > 0) {
                logger.debug("Reading documents ({}/{}) query: {}", count, total, query);
            }

            query.set("cursorMark", nextCursor);
            if (nextCursor.equals(cursorMark)) {
                isDone = true;
            }
            cursorMark = nextCursor;

        }
        logger.info("Finished reading documents ({}/{}) for query: {}", count, total, query);
        return true;
    }

    /*    //TODO: find why only top-n results are returned
    public void queryWithStream(String collection, String queryStr, String[] fields, ICallback callback) throws IOException {
        String sort = "id asc";
        String otherFields = String.join(",", Arrays.asList(fields)).replace(",id,", "").replace(",,", ",");
        String fl = String.format("\"id,%s\"", otherFields);
//        String cexpr = String.format("select("
//                + "search(%s,fl=%s,q=%s,sort=%s), id as id, %s"
//                + ")", collection, fl, queryStr, sort, otherFields);

        String cexpr = String.format("search(%s,fl=%s,q=%s,sort=%s)", collection, fl, queryStr, sort);

        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set("expr", cexpr);
        paramsLoc.set("qt", "/stream");
        // Note, the "/collection" below can be an alias.
        String url = "http://10.10.40.183:31000/solr" + "/" + collection;
        TupleStream solrStream = new SolrStream(url, paramsLoc);
        StreamContext context = new StreamContext();
        solrStream.setStreamContext(context);
        solrStream.open();
        long count = 0;
        Tuple docStream = null;
        try {
            docStream = solrStream.read();
        } catch (IOException ex) {
            logger.error("Failed to execute streaming expression {}", cexpr, ex);
        }
        ArrayList<SolrInputDocument> docList = new ArrayList<>();
        while (docStream.EOF == false) {
            Map docFields = docStream.getMap();
            SolrInputDocument solrDoc = new SolrInputDocument();
            docFields.keySet().stream().forEach(docField -> {
                Object value = docFields.get(docField);
                solrDoc.addField((String) docField, value);
            });

            docList.add(solrDoc);

            if (callback != null && docList.size() == batchSize) {
                callback.execute(docList);
                docList.clear();
            }
            docStream = solrStream.read();
            count += 1;
        }

        if (callback != null && docList.size() > 0) {
            callback.execute(docList);
        }

        solrStream.close(); // could be try-with-resources

    }
     */
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
                getCloudSolrClient().add(collection, docs);
            }
        } else {
            logger.error("Failed pushing documents");
        }
    }

    public void indexDocuments(String collection, ArrayList<SolrInputDocument> docList) throws SolrServerException, IOException {
        List<SolrInputDocument> docs = new ArrayList<>();
        if (docList != null) {
//            docList.parallelStream().forEach(doc -> {
//                doc.removeField("_version_");                
//            });
            //push the list of documents to Solr
            getCloudSolrClient().add(collection, docList);
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

    protected SolrClient getCloudSolrClient() {
        SolrClient localCloudSolrClient;

        int randomId = random.nextInt(numSolrClient);
        if (solrClientMap.containsKey(randomId) == false) {
            List<String> hosts = Arrays.asList(zkHost);
            CloudSolrClient solrClient = new CloudSolrClient.Builder().withZkHost(hosts).build();
            solrClientMap.put(randomId, solrClient);
        }
        localCloudSolrClient = solrClientMap.get(randomId);
        return localCloudSolrClient;
    }

}

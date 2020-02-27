package br.gov.bndes.pratikai;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;

/* Example class showing the skeleton of using Tika and
   Sql on the client to index documents from
   both structured documents and a SQL database.

   NOTE: The SQL example and the Tika example are entirely orthogonal.
   Both are included here to make a
   more interesting example, but you can omit either of them.

 */
public class OriginalSqlTikaExample {
  private CloudSolrClient client;
  private long start = System.currentTimeMillis();
  private AutoDetectParser autoParser;
  private int totalTika = 0;
  private int totalSql = 0;
  
  private final String zkEnsemble = "http://localhost:2181";

  private Collection docList = new ArrayList();

  public static void main(String[] args) {
    try {
      OriginalSqlTikaExample idxer = new OriginalSqlTikaExample("http://localhost:8983/solr");

      idxer.doTikaDocuments(new File("/Users/Erick/testdocs"));
      idxer.doSqlDocuments();

      idxer.endIndexing();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private OriginalSqlTikaExample(String url) throws IOException, SolrServerException {
    // Create a SolrCloud-aware client to send docs to Solr
    // Use something like HttpSolrClient for stand-alone
    
    client = new CloudSolrClient.Builder().withZkHost(zkEnsemble).build();

    // Solr 8 uses a builder pattern here.
    //     client = new CloudSolrClient.Builder(Collections.singletonList(zkEnsemble), Optional.empty())
    //    .withConnectionTimeout(5000)
    //    .withSocketTimeout(10000)
    //    .build();


    // binary parser is used by default for responses
    client.setParser(new XMLResponseParser());

    // One of the ways Tika can be used to attempt to parse arbitrary files.
    autoParser = new AutoDetectParser();
  }

  // Just a convenient place to wrap things up.
  private void endIndexing() throws IOException, SolrServerException {
    if ( docList.size() > 0) { // Are there any documents left over?
      client.add(docList, 300000); // Commit within 5 minutes
    }
    client.commit(); // Only needs to be done at the end,
    // commitWithin should do the rest.
    // Could even be omitted
    // assuming commitWithin was specified.
    long endTime = System.currentTimeMillis();
    log("Total Time Taken: " + (endTime - start) +
        " milliseconds to index " + totalSql +
        " SQL rows and " + totalTika + " documents");
  }

  // I hate writing System.out.println() everyplace,
  // besides this gives a central place to convert to true logging
  // in a production system.
  private static void log(String msg) {
    System.out.println(msg);
  }

  /**
   * ***************************Tika processing here
   */
  // Recursively traverse the filesystem, parsing everything found.
  private void doTikaDocuments(File root) throws IOException, SolrServerException {

    // Simple loop for recursively indexing all the files
    // in the root directory passed in.
    for (File file : root.listFiles()) {
      if (file.isDirectory()) {
        doTikaDocuments(file);
        continue;
      }
      // Get ready to parse the file.
      ContentHandler textHandler = new BodyContentHandler();
      Metadata metadata = new Metadata();
      ParseContext context = new ParseContext();
      // Tim Allison noted the following, thanks Tim!
      // If you want Tika to parse embedded files (attachments within your .doc or any other embedded 
      // files), you need to send in the autodetectparser in the parsecontext:
      // context.set(Parser.class, autoParser);

      InputStream input = new FileInputStream(file);

      // Try parsing the file. Note we haven't checked at all to
      // see whether this file is a good candidate.
      try {
        autoParser.parse(input, textHandler, metadata, context);
      } catch (Exception e) {
        // Needs better logging of what went wrong in order to
        // track down "bad" documents.
        log(String.format("File %s failed", file.getCanonicalPath()));
        e.printStackTrace();
        continue;
      }
      // Just to show how much meta-data and what form it's in.
      dumpMetadata(file.getCanonicalPath(), metadata);

      // Index just a couple of the meta-data fields.
      SolrInputDocument doc = new SolrInputDocument();

      doc.addField("id", file.getCanonicalPath());

      // Crude way to get known meta-data fields.
      // Also possible to write a simple loop to examine all the
      // metadata returned and selectively index it and/or
      // just get a list of them.
      // One can also use the Lucidworks field mapping to
      // accomplish much the same thing.
      String author = metadata.get("Author");

      if (author != null) {
        doc.addField("author", author);
      }

      doc.addField("text", textHandler.toString());

      docList.add(doc);
      ++totalTika;

      // Completely arbitrary, just batch up more than one document
      // for throughput!
      if ( docList.size() >= 1000) {
        // Commit within 5 minutes.
        UpdateResponse resp = client.add(docList, 300000);
        if (resp.getStatus() != 0) {
          log("Some horrible error has occurred, status is: " +
              resp.getStatus());
        }
        docList.clear();
      }
    }
  }

  // Just to show all the metadata that's available.
  private void dumpMetadata(String fileName, Metadata metadata) {
    log("Dumping metadata for file: " + fileName);
    for (String name : metadata.names()) {
      log(name + ":" + metadata.get(name));
    }
    log("nn");
  }

  /**
   * ***************************SQL processing here
   */
  private void doSqlDocuments() throws SQLException {
    Connection con = null;
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      log("Driver Loaded......");

      con = DriverManager.getConnection("jdbc:mysql://192.168.1.103:3306/test?"
          + "user=testuser&password=test123");

      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery("select id,title,text from test");

      while (rs.next()) {
        // DO NOT move this outside the while loop
        SolrInputDocument doc = new SolrInputDocument();
        String id = rs.getString("id");
        String title = rs.getString("title");
        String text = rs.getString("text");

        doc.addField("id", id);
        doc.addField("title", title);
        doc.addField("text", text);

        docList.add(doc);
        ++totalSql;

        // Completely arbitrary, just batch up more than one
        // document for throughput!
        if ( docList.size() > 1000) {
          // Commit within 5 minutes.
          UpdateResponse resp = client.add(docList, 300000);
          if (resp.getStatus() != 0) {
            log("Some horrible error has occurred, status is: " +
                resp.getStatus());
          }
          docList.clear();
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (con != null) {
        con.close();
      }
    }
  }
}

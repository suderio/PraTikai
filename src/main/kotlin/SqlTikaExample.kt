package br.gov.bndes.pratikai

import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.impl.XMLResponseParser
import org.apache.solr.common.SolrInputDocument
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.sax.BodyContentHandler
import java.io.FileInputStream
import java.io.IOException
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException

/* Example class showing the skeleton of using Tika and
   Sql on the client to index documents from
   both structured documents and a SQL database.

   NOTE: The SQL example and the Tika example are entirely orthogonal.
   Both are included here to make a
   more interesting example, but you can omit either of them.

 */
class SqlTikaExample private constructor(url: String) {
    private val client: CloudSolrClient
    //private val start: Long = java.lang.System.currentTimeMillis()
    private val autoParser: AutoDetectParser
    private var totalTika = 0
    private var totalSql = 0
    private val zkEnsemble = "http://localhost:2181"
    private val docList: MutableCollection<SolrInputDocument> = java.util.ArrayList()
    // Just a convenient place to wrap things up.
    @Throws(IOException::class, SolrServerException::class)
    private fun endIndexing() {
        // Are there any documents left over?
        if (docList.isNotEmpty()) {
            client.add(docList, 300000) // Commit within 5 minutes
        }
        client.commit()
        // Only needs to be done at the end,
        // commitWithin should do the rest.
        // Could even be omitted
        // assuming commitWithin was specified.
        val endTime: Long = java.lang.System.currentTimeMillis()
        log("Total Time Taken: " + (endTime - start) +
                " milliseconds to index " + totalSql +
                " SQL rows and " + totalTika + " documents")
    }

    /**
     * Tika processing here
     */
    // Recursively traverse the filesystem, parsing everything found.
    @Throws(IOException::class, SolrServerException::class)
    private fun doTikaDocuments(root: java.io.File) {
        // Simple loop for recursively indexing all the files
        // in the root directory passed in.
        for (file in root.listFiles()) {
            if (file.isDirectory) {
                doTikaDocuments(file)
                continue
            }
            // Get ready to parse the file.
            val textHandler: org.xml.sax.ContentHandler = BodyContentHandler()
            val metadata: org.apache.tika.metadata.Metadata = org.apache.tika.metadata.Metadata()
            val context: org.apache.tika.parser.ParseContext = org.apache.tika.parser.ParseContext()
            // Tim Allison noted the following, thanks Tim!
            // If you want Tika to parse embedded files (attachments within your .doc or any other embedded
            // files), you need to send in the autodetectparser in the parsecontext:
            // context.set(Parser.class, autoParser);
            val input: java.io.InputStream = FileInputStream(file)
            // Try parsing the file. Note we haven't checked at all to
            // see whether this file is a good candidate.
            try {
                autoParser.parse(input, textHandler, metadata, context)
            } catch (e: java.lang.Exception) {
                // Needs better logging of what went wrong in order to
                // track down "bad" documents.
                log(String.format("File %s failed", file.canonicalPath))
                e.printStackTrace()
                continue
            }
            // Just to show how much meta-data and what form it's in.
            dumpMetadata(file.canonicalPath, metadata)
            // Index just a couple of the meta-data fields.
            val doc = SolrInputDocument()
            doc.addField("id", file.canonicalPath)
            // Crude way to get known meta-data fields.
            // Also possible to write a simple loop to examine all the
            // metadata returned and selectively index it and/or
            // just get a list of them.
            // One can also use the Lucidworks field mapping to
            // accomplish much the same thing.
            val author: String = metadata.get("Author")
            if (author != null) {
                doc.addField("author", author)
            }
            doc.addField("text", textHandler.toString())
            docList.add(doc)
            ++totalTika
            // Completely arbitrary, just batch up more than one document
            // for throughput!
            if (docList.size >= 1000) {
                // Commit within 5 minutes.
                val resp: org.apache.solr.client.solrj.response.UpdateResponse = client.add(docList, 300000)
                if (resp.status != 0) {
                    log("Some horrible error has occurred, status is: " +
                            resp.status)
                }
                docList.clear()
            }
        }
    }

    // Just to show all the metadata that's available.
    private fun dumpMetadata(fileName: String, metadata: org.apache.tika.metadata.Metadata) {
        log("Dumping metadata for file: $fileName")
        for (name in metadata.names()) {
            log(name + ":" + metadata.get(name))
        }
        log("nn")
    }

    /**
     * SQL processing here
     */
    @Throws(SQLException::class)
    private fun doSqlDocuments() {
        var con: java.sql.Connection? = null
        try {
            java.lang.Class.forName("com.mysql.jdbc.Driver").newInstance()
            log("Driver Loaded......")
            con = DriverManager.getConnection("jdbc:mysql://192.168.1.103:3306/test?"
                    + "user=testuser&password=test123")
            val st: java.sql.Statement = con.createStatement()
            val rs: ResultSet = st.executeQuery("select id,title,text from test")
            while (rs.next()) { // DO NOT move this outside the while loop
                val doc = SolrInputDocument()
                val id: String = rs.getString("id")
                val title: String = rs.getString("title")
                val text: String = rs.getString("text")
                doc.addField("id", id)
                doc.addField("title", title)
                doc.addField("text", text)
                docList.add(doc)
                ++totalSql
                // Completely arbitrary, just batch up more than one
                // document for throughput!
                if (docList.size > 1000) {
                    // Commit within 5 minutes.
                    val resp: org.apache.solr.client.solrj.response.UpdateResponse = client.add(docList, 300000)
                    if (resp.status != 0) {
                        log("Some horrible error has occurred, status is: " +
                                resp.status)
                    }
                    docList.clear()
                }
            }
        } catch (ex: java.lang.Exception) {
            ex.printStackTrace()
        } finally {
            con?.close()
        }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val idxer = SqlTikaExample("http://localhost:8983/solr")
                idxer.doTikaDocuments(java.io.File("/Users/Erick/testdocs"))
                idxer.doSqlDocuments()
                idxer.endIndexing()
            } catch (e: java.lang.Exception) {
                e.printStackTrace()
            }
        }

        // I hate writing System.out.println() everyplace,
        // besides this gives a central place to convert to true logging
        // in a production system.
        private fun log(msg: String) {
            println(msg)
        }
    }

    init {
        // Create a SolrCloud-aware client to send docs to Solr
        // Use something like HttpSolrClient for stand-alone
        client = CloudSolrClient.Builder().withZkHost(zkEnsemble).build()
        // Solr 8 uses a builder pattern here.
        //     client = new CloudSolrClient.Builder(Collections.singletonList(zkEnsemble), Optional.empty())
        //    .withConnectionTimeout(5000)
        //    .withSocketTimeout(10000)
        //    .build();
        // binary parser is used by default for responses
        client.parser = XMLResponseParser()
        // One of the ways Tika can be used to attempt to parse arbitrary files.
        autoParser = AutoDetectParser()
    }
}
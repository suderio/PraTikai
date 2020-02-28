package br.gov.bndes.pratikai

import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.impl.XMLResponseParser
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.SolrInputDocument
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler
import org.xml.sax.ContentHandler
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.lang.Exception
import java.sql.*
import java.util.*


fun main(args: Array<String>) {
    try {
        val idxer = SqlTikaExample("http://localhost:8983/solr")
        idxer.doTikaDocuments(java.io.File("/Users/Erick/testdocs"))
        idxer.doSqlDocuments()
        idxer.endIndexing()
    } catch (e: Exception) {
        e.printStackTrace()
    }
}

// Just to show all the metadata that's available.
fun dumpMetadata(fileName: String, metadata: Metadata) {
    println("Dumping metadata for file: $fileName")
    for (name in metadata.names()) {
        println(name + ":" + metadata.get(name))
    }
    println("nn")
}

/* Example class showing the skeleton of using Tika and
   Sql on the client to index documents from
   both structured documents and a SQL database.

   NOTE: The SQL example and the Tika example are entirely orthogonal.
   Both are included here to make a
   more interesting example, but you can omit either of them.

 */
class SqlTikaExample(url: String) {
    private val client: CloudSolrClient
    private val start: Long = System.currentTimeMillis()
    private val autoParser: AutoDetectParser
    private var totalTika = 0
    private var totalSql = 0
    private val zkEnsemble = "http://localhost:2181"
    private val docList: MutableCollection<SolrInputDocument> = ArrayList()

    init {
        // Create a SolrCloud-aware client to send docs to Solr
        // Use something like HttpSolrClient for stand-alone
        // client = CloudSolrClient.Builder().withZkHost(zkEnsemble).build()
        // Solr 8 uses a builder pattern here.
        client =  CloudSolrClient.Builder(Collections.singletonList(zkEnsemble), Optional.empty())
                .withConnectionTimeout(5000)
                .withSocketTimeout(10000)
                .build();
        // binary parser is used by default for responses
        client.parser = XMLResponseParser()
        // One of the ways Tika can be used to attempt to parse arbitrary files.
        autoParser = AutoDetectParser()
    }

    // Just a convenient place to wrap things up.
    @Throws(IOException::class, SolrServerException::class)
    fun endIndexing() {
        // Are there any documents left over?
        if (docList.isNotEmpty()) {
            client.add(docList, 300000) // Commit within 5 minutes
        }
        client.commit()
        // Only needs to be done at the end,
        // commitWithin should do the rest.
        // Could even be omitted
        // assuming commitWithin was specified.
        val endTime: Long = System.currentTimeMillis()
        println("Total Time Taken: ${endTime - start} milliseconds to index $totalSql SQL rows and $totalTika documents")
    }

    /**
     * Tika processing here
     */
    // Recursively traverse the filesystem, parsing everything found.
    @Throws(IOException::class, SolrServerException::class)
    fun doTikaDocuments(root: java.io.File) {
        // Simple loop for recursively indexing all the files
        // in the root directory passed in.
        root.listFiles().forEach { file ->
            if (file.isDirectory) {
                doTikaDocuments(file)
                return@forEach
            }
            // Get ready to parse the file.
            val textHandler: ContentHandler = BodyContentHandler()
            val metadata: Metadata = Metadata()
            val context: ParseContext = ParseContext()
            // If you want Tika to parse embedded files (attachments within your .doc or any other embedded
            // files), you need to send in the autodetectparser in the parsecontext:
            // context.set(Parser.class, autoParser);
            val input: InputStream = FileInputStream(file)
            // Try parsing the file. Note we haven't checked at all to
            // see whether this file is a good candidate.
            try {
                autoParser.parse(input, textHandler, metadata, context)
            } catch (e: Exception) {
                // Needs better logging of what went wrong in order to
                // track down "bad" documents.
                println(String.format("File %s failed", file.canonicalPath))
                e.printStackTrace()
                return@forEach
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
            val author = metadata.get("Author")
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
                val resp: UpdateResponse = client.add(docList, 300000)
                if (resp.status != 0) {
                    println("Some horrible error has occurred, status is: ${resp.status}")
                }
                docList.clear()
            }
        }
    }

    /**
     * SQL processing here
     */
    @Throws(SQLException::class)
    fun doSqlDocuments() {
        var con: Connection? = null
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
            println("Driver Loaded......")
            con = DriverManager.getConnection("jdbc:mysql://192.168.1.103:3306/test?user=testuser&password=test123")
            val st: Statement = con.createStatement()
            val rs: ResultSet = st.executeQuery("select id,title,text from test")
            while (rs.next()) {
                // DO NOT move this outside the while loop
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
                    val resp: UpdateResponse = client.add(docList, 300000)
                    if (resp.status != 0) {
                        println("Some horrible error has occurred, status is: ${resp.status}")
                    }
                    docList.clear()
                }
            }
        } catch (ex: Exception) {
            ex.printStackTrace()
        } finally {
            con?.close()
        }
    }
}
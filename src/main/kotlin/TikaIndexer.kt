package br.gov.bndes.pratikai

import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.impl.XMLResponseParser
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.SolrInputDocument
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler
import org.xml.sax.ContentHandler
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.attribute.AclFileAttributeView
import java.util.*


fun main() {
    try {
        val idxer = TikaIndexer("http://localhost:8983/solr")
        idxer.doTikaDocuments(File("/some/docs"))
        idxer.endIndexing()
    } catch (e: Exception) {
        e.printStackTrace()
    }
}

// Just to show all the metadata that's available.
fun dumpMetadata(fileName: String, metadata: Metadata) {
    println("Dumping metadata for file: $fileName")
    metadata.names().forEach {
        println(it + ":" + metadata.get(it))
    }
    println("\n\n")
}

class TikaIndexer(url: String) {
    private val client: HttpSolrClient = HttpSolrClient.Builder()
            .withConnectionTimeout(5000)
            .withSocketTimeout(10000)
            .build()
    private val start: Long = System.currentTimeMillis()
    private val autoParser: AutoDetectParser
    private var totalTika = 0
    private var totalSql = 0
    private val docList: MutableCollection<SolrInputDocument> = ArrayList()

    init {
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
    fun doTikaDocuments(root: File) {
        // Simple loop for recursively indexing all the files
        // in the root directory passed in.
        root.listFiles().forEach { file ->
            if (file.isDirectory) {
                doTikaDocuments(file)
                return@forEach
            }
            // Show ACL of the file
            val aclFileAttributes: AclFileAttributeView = Files.getFileAttributeView(
                    file.toPath(), AclFileAttributeView::class.java)

            aclFileAttributes.acl.forEach { aclEntry ->
                println(aclEntry.principal().toString() + ":")
                println(aclEntry.permissions().toString() + "\n")
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
}
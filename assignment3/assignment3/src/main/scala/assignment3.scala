import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark.SparkConf

import scala.xml.XML
import java.io.File
import scala.util.parsing.json._
import scalaj.http._
import scala.collection.mutable.MutableList

object CaseIndex{
    def main(args:Array[String]){
        val inputFilePath = args(0)
        val conf = new SparkConf().setAppName("CaseIndex").setMaster("local")
        val sc = new SparkContext(conf)
        //1.create elastice search index and mapping
        val create_index = Http("http://localhost:9200/legal_idx?pretty").
                        method("PUT").asString
        val mapping = "{\"cases\": {\"properties\": {\"name\": {\"type\": \"text\"},\"AustLII\": {\"type\": \"text\"},\"catchphrases\": {\"type\": \"text\"},\"sentences\": {\"type\": \"text\"},\"person\": {\"type\": \"text\"},\"location\": {\"type\": \"text\"},\"organization\": {\"type\": \"text\"}}}}"
        val create_mapping = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty").
                            method("PUT").header("content-type", "application/json").
                            postData(mapping).asString
        
        //2.Loop through all input files and put corresponding entities up to ElasticSearch Server
        val xmlFilesFolder = new File(inputFilePath)
        val xmlFileList = xmlFilesFolder.listFiles().filter{ f => f.isFile() && f.getName.endsWith(".xml")}.toList
        for(file <- xmlFileList){
            //3.extract file content
            val file_name = file.getName().split(".xml")(0)
            val xmlFile = XML.loadFile(file)
            val temp_name = (xmlFile \\ "name" ).text
            val temp_Aust = (xmlFile \\ "AustLII" ).text
            val temp_catch = (xmlFile \\ "catchphrases" ).text.trim().split("\n").map(s=>s.trim()).mkString(" ")//trim:remove space at the beginning and end of contxt,split:transform to array
            val temp_sen = (xmlFile \\ "sentences" ).text.trim().split("\n").map(s => s.trim()).mkString(" ")
            val concatString = temp_name + " " + temp_Aust + " " + temp_catch + " " + temp_sen 
            //4.put file content up to coreNlp server and obtain response as a json string   tokenize%2Cssplit%2Cpos%2C
            val curlResult = Http("""http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D""").
                            method("POST").timeout(connTimeoutMs = 120000, readTimeoutMs = 120000).postData(concatString).asString.body
            //5.parse the json string based on this structur:
            //response body structure:"{sentence : [ {index:0,tokens:[]}]}"
            val curlResultoJson = JSON.parseFull(curlResult).get.asInstanceOf[Map[String, List[Map[String, Any]]]]
            var person = ""
            var location = ""
            var org = ""
            val LocCatches = MutableList[String]()
            val OrgCatches = MutableList[String]()
            for(c <- curlResultoJson("sentences")){
                val entity = c("tokens").asInstanceOf[List[Map[String,String]]]
                for(e <- entity){
                    val ner = e("ner")
                    val text = e("originalText")
                    if(ner == "PERSON"){
                        person += text + " "
                    }
                    if(ner == "LOCATION" && !(LocCatches.contains(text))){
                        LocCatches += text
                        location += text + " "
                    }
                    if(ner == "ORGANIZATION" && !(OrgCatches.contains(text))){
                        org += text + " "
                        OrgCatches += text
                    }
                }
            }
            //postprocessing dealing with double quoates and whitespaces
            person = person.trim()
            location = location.trim()
            org = org.trim()
            val name_modi = temp_name.replace("\"","\'")
            val aus_modi = temp_Aust.replace("\"","\'")
            val sen_modi = temp_sen.replace("\"","\'")
            val cach_modi = temp_catch.replace("\"","\'")
            //construct json object
            val jsonString = s"""{"name":"$temp_name",
                                "AustLII":"$temp_Aust",
                                "catchphrases":"$temp_catch",
                                "sentences":"$sen_modi",
                                "person":"$person",
                                "location":"$location",
                                "organization":"$org"}"""
            //6.push to index in elasticsearch
            val pushToEs = Http(f"http://127.0.0.1:9200/legal_idx/cases/$file_name%s?pretty").
                method("PUT").
                timeout(connTimeoutMs = 120000, readTimeoutMs = 120000).
                header("content-type", "application/json").
                postData(jsonString).asString
            //println(pushToEs)
        }
    }
}

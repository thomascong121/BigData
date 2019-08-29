// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "/import/adams/2/ccon404/workspace/scala_workspace/sample_input.txt"
val outputDirPath = "/import/adams/2/ccon404/workspace/scala_workspace/output"
// Write your solution here
val lines = sc.textFile(inputFilePath)
//Unify Data Type(MB/KB -> B)
val words = lines.map(s => s.split(",")).map(x => 
	if(x.last.takeRight(2) == "KB") 
	  (x(0),x.last.substring(0,x.last.length()-2).toInt*1024) 
	else if (x.last.takeRight(2) == "MB")
	  (x(0),x.last.substring(0,x.last.length()-2).toInt*1024*1024) 
	else 
	  (x(0),x.last.substring(0,x.last.length()-1)))  
//main calculation   
val wordList = words.groupByKey().map(x => {
    val ll = x._2.toList.map(_.toString.toInt);
    val min:Long = ll.min; val max:Long = ll.max; val len = ll.length; val sum = ll.sum; val meanPay:Double = sum/len;
    val variance = ll.map(_.toDouble).map(e => (e-meanPay)*(e-meanPay)).sum/len;
    (x._1,min+"B",max+"B","%.0fB,%.0fB".format(math.floor(meanPay),math.floor(variance)))})
//remove tuple brackets and output
val results = wordList.map(x => x.productIterator.mkString(","))
results.coalesce(1).saveAsTextFile(outputDirPath)
sc.stop()
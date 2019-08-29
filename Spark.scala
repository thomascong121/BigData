//wordcount in scala
val file = sc.textFile(path)
val count = file.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
count.saveAsTextFile(path)


val l = List(1,2,3,4)
l.map(x => x*2)//(2,4,6,8)
l.map(_*2)//(2,4,6,8)

l.reduce(_+_)//10

l.filer(_%2==0)//(2,4)

l.take(3)//4

val ll = List(List(1,2),List(3,4))
ll.flatMap(x => x.map(_*2))//2,4,6,8

val a = List(1,2,3)
val b = List(4,5,6)

a.zip(b)//List((1,4), (2,5), (3,6))

l.foldLeft(0){_+_}//10


q3.val q3 = file.map(x => x.split(",")).map(y => (y(0),1)).reduceByKey(_+_).collect

q4.val q4 = file.map(x => x.split(",")).map(y => (y(0),1)).reduceByKey(_+_).reduce((a,b) => if(a._2 > b._2) a else b)._2

q5.val q4 = file.map(x => x.split(",")).map(y => (y(0),1)).reduceByKey(_+_).sortBy(_._2,false).take(5)

##load from HDFs
val file = sc.textFile("path")
file.flatMap(x => x.split(" ")).map(word => (word,1)).reduceByKey(_+_).collect//.saveAsTextFile(path)


curl -X put "http://localhost:9200/indexName"
curl -X get "http://localhost:9200/_cat/indicies?v"
curl -X put "http://localhost:9200/indexName/TypeName/_mapping" -H ....
curl -X put "http://localhost:9200/indexName/TypeName/DocName"
curl -X get "http://localhost:9200/indexName/TypeName/_search"
curl -X post "http://localhost:9200/indexName/TypeName/DocName"

curl -X get "http://localhost:9200/indexName/TypeName/_search?q="BSD""
curl -X get "http://localhost:9200/indexName/TypeName/-search?q=description:(TCP%20AND%20BSD)"
curl -X get "http://localhost:9200/indexName/TypeName/_search?q=(description:(TCP%20OR%20BSD)%20AND%20cvss_score:>40)"
curl -X get "http://localhost:9200/indexName/TypeName/_search?q=description:%20TCP%20-NFS"


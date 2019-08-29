public static class Mymapper extends Mapper<Object,Text,Text,IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	//mapper
	pulic void mapper(Object key, Text value, Context context) throws IOException, InterruptedException{
		// StringTokenizer itr = new StringTokenizer(value.toString);
		// while (itr.hasMoreTokens()){
		// 	word.set(itr.nextToken());
		// 	context.write(word,one);
		// }
		StringTokenizer itr = new StringTokenizer(value.toString);
		while(itr.hasMoreTokens()){
			word.set(itr.nextToken());
			context.write(word,one);
		}
	}
}

public static class Mypartitioner extends Partitioner<Text, IntWritable>{
	@Override
	public int getpartition(Text key, IntWritable value, int numReduceTasks){
		if(numReduceTasks == 0){
			return 0;
		}
		if(keyChar0 == 'a'){
			return 0;
		}else if(keyChar0 == 'b'){
			return 1%numReduceTasks;
		}else{
			return 2%numReduceTasks;
		}
	}
}

public static class Myreducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWriable result = new IntWriable();

	public void reducer(Text key, Iterable<IntWriable> values, Context context) throws IOException,InterruptedException{
		int sum = 0;
		for (IntWriable i : values){
			sum += i.get();
		}
		result.set(sum);
		context.write(key,sum);

	}
}

Practice-1:

Counting total enrollments of two specified courses

Input Files: A list of students with their enrolled courses
	Jamie: COMP9313, COMP9318
	Tom: COMP9331, COMP9313
	… …

Mapper: 
	private final static IntWriable one = new IntWriable(1);
	private Text text = new Text();

	String lines = value.toString();
	String words = lines.split(":")[1];
	String[] course = words.split(",");

	for (String i : course){
		text.set(i);
		context.write(text,one)
	}
	(COMP9313,1),(COMP9318,1),(COMP9331,1),(COMP9313,1)
	//shuffle and sort:
	(COMP9313,[1,1]),(COMP9318,1),(COMP9331,1)

Reducer:
	int sum = 0;
	for (IntWriable i : values){
		sum += i.get()
	}
	context.write(key,sum)


Practice-2:

Remove duplicate records
Input: a list of records
	2013-11-01 aa	
	2013-11-02 bb	
	2013-11-03 cc	
	2013-11-01 aa	
	2013-11-03 dd

Mapper:
	Text text = new Text();
	Text empty = new Text("");
	String line = value.toString();
	text.set(line)
	context.write(text,empty);
	(2013-11-01 aa,["",""]),(2013-11-02 bb,[""]),(2013-11-03,[""]),(2013-11-03 dd,[""])

Reducer:
	context.write(key,new Text(""));


Practice-3:

Assume that in an online shopping system, a huge log file stores the information of each transaction. 
Each line of the log is in format of “userID\t product\t price\t time”. 
Your task is to use MapReduce to find out the top-5 expensive products purchased by each user in 2016

Mapper:
	IntWriable price = new IntWriable();
	Text Uid = new Text();
	Map map = new HashMap();

	String lines = value.toString();
	String[] content = line.split("\t");
	//generate user id as key
	//a map[price:product] for all products purchased by a user
	if(content[3] == "2016"){
		Uid.set(content[0]);
		map.put(content[2],content[1]);
	}
	context.write(Uid, new MapWritable(map))

	(Uid,[price:product]).....

Reducer:
	//sort map and take top 5
	String temp = "";
	Text prod = new Text();

	Object[] key = values.keySet().toArray();
	Arrays.sort(key);
	for (int i = 0; i < 5; i++){
		temp += values.get(key[i]);
	}

	context.write(key,prod.set(temp))



##
Partitioner Task

The partitioner task accepts the key-value pairs from the map task as its input. 
Partition implies dividing the data into segments. 
According to the given conditional criteria of partitions, 
if(some > 0){
	return 0;
}
else if(some == 0){
	return 1 % numReduceTasks;
}
else{
	return 2 % numReduceTasks;
}



##set up combiner and partitioner class
job.setCombinerClass(IntSumReducer.class);
job.setPartitionerClass(CaderPartitioner.class);






















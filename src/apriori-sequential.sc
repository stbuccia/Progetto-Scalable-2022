
val transactions = List(
  Set("A", "B", "C", "D"),
  Set("A" ,"B", "D"), 
  Set("A", "B"), 
  Set("B", "C", "D"), 
  Set("C", "D"), 
  Set("B", "D"), 
)

//TODO: Capire perché dà questi problemi se aumentiano a più nodi (controllare shuffling e partitioning sulle slide)
def phase1(transactionsRdd: RDD[Set[String]]) = {
  transactionsRdd.flatMap(itemset => itemset.map(item => (Set(item), 1))).reduceByKey((x, y) => x + y).filter(item => item._2 > 2)
}

@tailrec
def phase2(transactionsRdd: RDD[Set[String]], k: Int, minimum: Int, setL: RDD[(Set[String], Int)]): RDD[Set[String]] = {
  val setL_strings = setL.map(_._1)

  val setC_k = setL_strings.cartesian(setL_strings)
    .map(tuples => tuples._1 | tuples._2)
    .filter(_.size == k)
    .distinct()

  val setL_k = setC_k.cartesian(transactionsRdd)
    .filter(tuple => tuple._1.subsetOf(tuple._2))
    .map(tuple => (tuple._1, 1))
    .reduceByKey((x, y) => x + y)
    .filter(item => item._2 > minimum) 

  if (setL_k.count() == 0)
    setC_k
  else 
    phase2(transactionsRdd, k + 1, minimum, setL_k)
  
}

val conf = new SparkConf().setAppName("apriori-sequential").setMaster("local[2]")
conf.set("spark.driver.allowMultipleContexts","true");
val sc = new SparkContext(conf)
val transactionsRdd = sc.parallelize(transactions)

val setL_1 = phase1(transactionsRdd)

val out = phase2(transactionsRdd, 2, 2, setL_1)
out.collect().foreach(println)

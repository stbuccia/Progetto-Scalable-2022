package associationrulelearning

import org.apache.spark.rdd.RDD


class FPGrowth(dataset: RDD[Set[String]]) extends Serializable with Apriori[RDD[Set[String]]] {

    var transactions: RDD[Set[String]] = dataset


    def run(): Unit = {

        //Convert RDD[Set[String]] in RDD[Array[String]]
        val transactionsRDD: RDD[Array[String]] = transactions.map(_.toArray)

        val fpg = new org.apache.spark.mllib.fpm.FPGrowth()
          .setMinSupport(minSupport)
          .setNumPartitions(10)

        val model = fpg.run(transactionsRDD)

        //        model.freqItemsets.collect().foreach { itemset =>
        //            println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
        //        }

        model.freqItemsets.collect().foreach{itemset =>
            val item = (itemset.items.toSet, itemset.freq.toInt)
            frequentItemsets = frequentItemsets + item}

        //        model.generateAssociationRules(minConfidence).collect().foreach { rule =>
        //            println(
        //                rule.antecedent.mkString("[", ",", "]")
        //                + " => " + rule.consequent .mkString("[", ",", "]")
        //                + ", " + rule.confidence)
        //        }

        model.generateAssociationRules(minConfidence).collect().foreach{rule =>
            associationRules = associationRules :+ (rule.antecedent.toSet, rule.consequent.toSet, rule.confidence)
        }

        printResults()
    }
}


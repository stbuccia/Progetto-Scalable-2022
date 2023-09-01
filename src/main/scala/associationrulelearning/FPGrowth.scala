package associationrulelearning

import org.apache.spark.rdd.RDD


class FPGrowth(dataset: RDD[Set[String]]) extends Serializable with Apriori[RDD[Set[String]]] {

    var transactions: RDD[Set[String]] = dataset


    def run(): RDD[(Set[String], Set[String], Double)] = {

        // Convert RDD[Set[String]] in RDD[Array[String]]
        val transactionsRDD: RDD[Array[String]] = transactions.map(_.toArray)

        val fpg = new org.apache.spark.mllib.fpm.FPGrowth()
          .setMinSupport(minSupport)
          .setNumPartitions(10)

        val model = fpg.run(transactionsRDD)

        val frequentItemsets: RDD[(Set[String], Int)] = model.freqItemsets.map{itemset =>
            (itemset.items.toSet, itemset.freq.toInt)}

        val associationRules: RDD[(Set[String], Set[String], Double)] = model.generateAssociationRules(minConfidence).map{rule =>
            (rule.antecedent.toSet, rule.consequent.toSet, rule.confidence)}

        associationRules
    }
}


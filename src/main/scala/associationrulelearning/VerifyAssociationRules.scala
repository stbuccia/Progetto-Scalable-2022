package associationrulelearning

import org.apache.spark.rdd.RDD

object VerifyAssociationRules {

  /**
   * Takes the association rules then generates their accuracy by counting occurences in the given dataset.
   * @param rules association rules generated by the algorithm
   * @param dataset all transaction where to test for accuracy
   * @return  the set of rules together with their accuracy
   */
    /*
  def verify(rules: RDD[(Set[String], Set[String], Double)], dataset: RDD[(Int, Set[String])]): RDD[(Set[String], Set[String], Double)] = {


    // Count occurences of all (AUB), where A => B is a rule from rules RDD

    val unionRulesList = rules.map(rule => rule._1.union(rule._2)).collect()

    val unionRulesWithSupport = unionRulesList.map{ rule =>
      val support = dataset.filter(transaction => rule.subsetOf(transaction._2)).count().toDouble
      (rule, support)
    }

//    val unionRulesList = rules.map(rule => (rule._1.union(rule._2), 0)).collect()
//
//    val unionRulesWithSupport = dataset
//      .flatMap(transaction =>
//        unionRulesList.filter(rule => rule._1.subsetOf(transaction._2))
//          .map(rule => (rule._1, 1))
//      )
//      .reduceByKey((x, y) => x + y)
//      .collect()


    // Count occurences of all (A), where A => B is a rule from rules RDD

    val antecedentRulesList = rules.map(rule => rule._1).collect()

    val antecedentRulesWithSupport = antecedentRulesList.map{ rule =>
      val support = dataset.filter(transaction => rule.subsetOf(transaction._2)).count().toDouble
      (rule, support)
    }

//    val antecedentRulesList = rules.map(rule => (rule._1, 0)).collect()
//
//    val antecedentRulesWithSupport = dataset
//      .flatMap(transaction =>
//        antecedentRulesList.filter(rule => rule._1.subsetOf(transaction._2))
//          .map(rule => (rule._1, 1))
//      )
//      .reduceByKey((x, y) => x + y)
//      .collect()


    // Return rules list together with accuracy

    rules.map{ rule =>
      val unionSupport = unionRulesWithSupport
        .filter(_._1 == rule._1.union(rule._2))
        .map(_._2)
        .head
      val antecedentSupport = antecedentRulesWithSupport
        .filter(_._1 == rule._1)
        .map(_._2)
        .head
      (rule._1, rule._2, unionSupport / antecedentSupport)
    }
  }
*/
  def verify(rules: RDD[(Set[String], Set[String], Double)], dataset: RDD[(Int, Set[String])]): RDD[(Set[String], Set[String], Double)] = {

    val rddSet: Array[Set[String]] = rules.flatMap(rule => Set(rule._1.union(rule._2), rule._1)).distinct().collect()

    val mappaSupport = dataset.flatMap(transaction =>
      rddSet.filter(s => s.subsetOf(transaction._2)).map(s => (s,1)))
      .reduceByKey((x, y) => x + y)
      .collectAsMap()

    // SOLO PER TEST ----------------------------------------------------
    val ruleTest = rules.take(1)(0)
    val supAnt = dataset.filter(transaction => ruleTest._1.subsetOf(transaction._2)).count().toDouble
    val supUn = dataset.filter(transaction => ruleTest._1.union(ruleTest._2).subsetOf(transaction._2)).count().toDouble
    println("TEST ------ "+ruleTest +"     "+ (supUn/supAnt))
    //----------------------------------------------------


    rules.map(rule => {
      val supUnion: Double = mappaSupport(rule._1.union(rule._2))
      val supAnt: Double = mappaSupport(rule._1)
      (rule._1, rule._2, supUnion/supAnt)
    })
  }

}

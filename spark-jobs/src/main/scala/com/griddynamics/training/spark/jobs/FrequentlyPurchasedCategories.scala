package com.griddynamics.training.spark.jobs

object FrequentlyPurchasedCategories {

  def main(args: Array[String]) {
    ProductTableLoader.executeQuery(
      "select categoryName, count(*) as purchasesPerCat from products group by categoryName order by purchasesPerCat desc limit 10",
      "spark_top_10_frequently_purchases_categories"
    )
  }
}
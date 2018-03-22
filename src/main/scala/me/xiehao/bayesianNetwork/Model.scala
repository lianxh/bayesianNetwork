package me.xiehao.bayesianNetwork

import org.apache.spark.sql.SparkSession

class Model(
  val fractionalWeights: Seq[FractionalWeight] = Seq()
) {

  def ++(model: Model): Model = new Model(
    fractionalWeights ++ model.fractionalWeights
  )
  def reset(randomVariables: Seq[RandomVariable]): Model =
    new Model(
      randomVariables.map(_.name).map(
        name =>
          fractionalWeights.find(_.name == name).get
      )
    )

  /**
    * this.fractionalWeights将使用和
    * model.fractionalWeights相同的部分
    * 覆盖自身
    * @param model
    * @return
    */
  def take(model: Model): Model = {
    if (model == null) {
      this
    } else {
      new Model(
        fractionalWeights.map(fractionalWeight => {
          fractionalWeight.take(
            model.getFractionalWeightByName(
              fractionalWeight.name
            )
          )
        })
      )
    }
  }

  def getFractionalWeightByName(name: String): FractionalWeight =
    fractionalWeights.find(_.name == name).get

  def checkpointAndCache(checkpointDir: String): Model = {
    if (checkpointDir == null) {
      fractionalWeights.foreach(fractionalWeight => {
        fractionalWeight.dataFrame.cache()
        fractionalWeight.dataFrame.count()
      })

      this
    } else {
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.setCheckpointDir(checkpointDir)

      new Model(
        fractionalWeights.map(fractionalWeight => {
          new FractionalWeight(
            fractionalWeight.randomVariable,
            fractionalWeight.dataFrame.checkpoint().cache()
          )
        })
      )
    }
  }
  def unCache: Unit = fractionalWeights.foreach(_.unCache)

  def toFractionalWeights: Seq[FractionalWeight] = fractionalWeights
  def toProbability(name: String): Probability =
    getFractionalWeightByName(name).toProbability
  def toProbability(names: Seq[String]): Probability =
    names.map(getFractionalWeightByName).
      map(_.toProbability).reduceLeft(
        (pb1, pb2) => pb1.join(pb2)
      )

  def save(filePath: String): Unit = {
    fractionalWeights.foreach(fractionalWeight => {
      fractionalWeight.dataFrame.write.format("parquet").save(
        s"${filePath}/${fractionalWeight.name}"
      )
    })
  }
  def save(
    filePath: String,
    fractionalWeights: Seq[FractionalWeight]
  ): Unit = {
    fractionalWeights.foreach(fractionalWeight => {
      fractionalWeight.dataFrame.write.format("parquet").save(
        s"${filePath}/${fractionalWeight.name}"
      )
    })
  }
}

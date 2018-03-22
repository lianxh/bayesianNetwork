package me.xiehao.bayesianNetwork

class Manipulate(
  val transforms: Seq[(Seq[(String, Seq[String])], String)] = Seq(),
  val actions: Seq[(Seq[(String, Seq[String])], Manipulate)] = Seq()
) {
  def ++(manipulate: Manipulate): Manipulate = new Manipulate(
    actions = actions ++ manipulate.actions,
    transforms = transforms ++ manipulate.transforms
  )

  private def calculate(
    model: Model,
    dataSet: DataSet,
    namesToBroadcast: Seq[String],
    debug: Boolean,
    seq: Seq[(String, Seq[String])]
  ): DataSet = {
    seq.foldLeft(dataSet){
      case (
        dataSetToCalculate,
        (
          nameToCalculate,
          namesToCoalesce
        )
      ) => {
        val fractionalWeight = model.getFractionalWeightByName(nameToCalculate)

        val dataSetBeforeCoalesced = dataSetToCalculate.calculate(
          fractionalWeight,
          namesToBroadcast.contains(fractionalWeight.name),
          debug
        )

        if (namesToCoalesce == null) dataSetBeforeCoalesced
        else dataSetBeforeCoalesced.coalesceByUnNormalRandomVariables(
          namesToCoalesce.map(model.getFractionalWeightByName).map(_.randomVariable),
          debug
        )
      }
    }
  }

  def apply(
    model: Model,
    dataSet: DataSet,
    namesToBroadcast: Seq[String],
    debug: Boolean,
    checkpointDir: String
  ): Model = {
    new Model(
      transforms.map{
        case (seq, nameToManipulate) =>
          model.getFractionalWeightByName(nameToManipulate).
            randomVariable.toFractionalWeight(
              calculate(
                model,
                dataSet,
                namesToBroadcast,
                debug,
                seq
              )
            )
      }
    ).checkpointAndCache(checkpointDir) ++
      actions.map{
        case (seq, manipulate) =>
          val dataSetAfterCalculated = calculate(
            model,
            dataSet,
            namesToBroadcast,
            debug,
            seq
          )

          dataSetAfterCalculated.dataFrame.cache()

          val modelAfterCalculated = manipulate(
            model,
            dataSetAfterCalculated,
            namesToBroadcast,
            debug,
            checkpointDir
          )

          dataSetAfterCalculated.dataFrame.unpersist()

          modelAfterCalculated
      }.foldLeft(new Model())(_ ++ _)
  }
}

object Manipulate {
  def transforms(values: Seq[(Seq[(String, Seq[String])], String)])
  : Manipulate = new Manipulate(transforms = values)

  def actions(values: Seq[(Seq[(String, Seq[String])], Manipulate)])
  : Manipulate = new Manipulate(actions = values)

  def transform(value: (Seq[(String, Seq[String])], String))
  : Manipulate = new Manipulate(transforms = Seq(value))
  def transform(value: String): Manipulate =
    new Manipulate(transforms = Seq((Seq(), value)))

  def action(value: (Seq[(String, Seq[String])], Manipulate))
  : Manipulate = new Manipulate(actions = Seq(value))
}

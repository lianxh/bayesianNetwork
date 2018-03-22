package me.xiehao.bayesianNetwork

import org.apache.spark.sql.SparkSession

import scala.util.Random

object Example {
  val checkpointDir = "/app/map/map-client/xiehao/spark/checkpoint"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local").
      getOrCreate()

    val maxTurn = try {
      args(1).toInt
    } catch {
      case _ => 2
    }
    val debug: Boolean = try {
      args(2) == "true"
    } catch {
      case _ => false
    }

    args(0) match {
      case "logic" => logic(spark, maxTurn, debug)
      case "types" => types(spark, maxTurn, debug)
      case "naive" => naive(spark, maxTurn, debug)
    }
  }

  def logic(spark: SparkSession, maxTurn: Int, debug: Boolean): Unit = {
    import spark.implicits._
    val a = RandomVariable.normalRandomVariable("a")
    val b = RandomVariable.normalRandomVariable("b")
    val c = RandomVariable.missingRandomVariable("c", (0 to 1))

    val dataFrameOfAND = Seq(
      (1, 1, new Integer(1)), (1, 0, new Integer(0)),
      (0, 1, null.asInstanceOf[Integer]), (0, 1, new Integer(0)),
      (0, 0, new Integer(0)), (1, 1, null.asInstanceOf[Integer]),
      (1, 1, new Integer(1)), (1, 1, null.asInstanceOf[Integer]),
      (0, 1, new Integer(0)), (1, 0, new Integer(0)),
      (1, 0, new Integer(0)), (0, 0, new Integer(0))
    ).toDF(a.name, b.name, c.name)
    val dataFrameOfOR = Seq(
      (1, 1, new Integer(1)), (1, 0, new Integer(1)),
      (0, 1, null.asInstanceOf[Integer]), (0, 1, new Integer(1)),
      (0, 0, new Integer(0)), (1, 1, null.asInstanceOf[Integer]),
      (1, 1, new Integer(1)), (0, 0, null.asInstanceOf[Integer]),
      (0, 1, new Integer(1)), (1, 0, new Integer(1)),
      (1, 0, new Integer(1)), (0, 0, new Integer(0))
    ).toDF(a.name, b.name, c.name)
    val dataFrameOfXOR = Seq(
      (1, 1, new Integer(0)), (1, 0, new Integer(1)),
      (0, 1, null.asInstanceOf[Integer]), (0, 1, new Integer(1)),
      (0, 0, new Integer(0)), (1, 1, null.asInstanceOf[Integer]),
      (1, 1, new Integer(0)), (0, 0, null.asInstanceOf[Integer]),
      (0, 1, new Integer(1)), (1, 0, new Integer(1)),
      (1, 0, new Integer(1)), (0, 0, new Integer(0))
    ).toDF(a.name, b.name, c.name)
    val dataFrameOfXNOR = Seq(
      (1, 1, new Integer(1)), (1, 0, new Integer(0)),
      (0, 1, null.asInstanceOf[Integer]), (0, 1, new Integer(0)),
      (0, 0, new Integer(1)), (1, 1, null.asInstanceOf[Integer]),
      (1, 1, new Integer(1)), (0, 0, null.asInstanceOf[Integer]),
      (0, 1, new Integer(0)), (1, 0, new Integer(0)),
      (1, 0, new Integer(0)), (0, 0, new Integer(1))
    ).toDF(a.name, b.name, c.name)

    val bayesianNetwork = new BayesianNetwork(Seq(
      a, b,
      c.addPi(a).addPi(b)
    ))

    val manipulate = new Manipulate(
      transforms = Seq(
        (
          Seq(
            (a.name, null),
            (b.name, null),
            (c.name, Seq(c.name))
          ),
          a.name
        ),
        (
          Seq(
            (a.name, null),
            (b.name, null),
            (c.name, Seq(c.name))
          ),
          b.name
        ),
        (
          Seq(
            (a.name, null),
            (b.name, null),
            (c.name, Seq(c.name))
          ),
          c.name
        )
      )
    )

    val modelOfAND = bayesianNetwork.fit(
      dataFrame = dataFrameOfAND,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug
    )
    val modelOfOR = bayesianNetwork.fit(
      dataFrame = dataFrameOfOR,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug
    )
    val modelOfXOR = bayesianNetwork.fit(
      dataFrame = dataFrameOfXOR,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug
    )
    val modelOfXNOR = bayesianNetwork.fit(
      dataFrame = dataFrameOfXNOR,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug
    )

    val probabilityOfAND = modelOfAND.toProbability(
      Seq(a.name, b.name, c.name)
    ).divideByNormal(Seq(a.name, b.name))
    val probabilityOfOR = modelOfOR.toProbability(
      Seq(a.name, b.name, c.name)
    ).divideByNormal(Seq(a.name, b.name))
    val probabilityOfXOR = modelOfXOR.toProbability(
      Seq(a.name, b.name, c.name)
    ).divideByNormal(Seq(a.name, b.name))
    val probabilityOfXNOR = modelOfXNOR.toProbability(
      Seq(a.name, b.name, c.name)
    ).divideByNormal(Seq(a.name, b.name))

    println("logic:AND")
    Check.flat(
      probabilityOfAND.dataFrame,
      probabilityOfAND.unNormalColumn.get.name,
      probabilityOfAND.unNormalColumn.get.len
    )
    println("logic:OR")
    Check.flat(
      probabilityOfOR.dataFrame,
      probabilityOfOR.unNormalColumn.get.name,
      probabilityOfOR.unNormalColumn.get.len
    )
    println("logic:XOR")
    Check.flat(
      probabilityOfXOR.dataFrame,
      probabilityOfXOR.unNormalColumn.get.name,
      probabilityOfXOR.unNormalColumn.get.len
    )
    println("logic:XNOR")
    Check.flat(
      probabilityOfXNOR.dataFrame,
      probabilityOfXNOR.unNormalColumn.get.name,
      probabilityOfXNOR.unNormalColumn.get.len
    )
  }

  def types(spark: SparkSession, maxTurn: Int, debug: Boolean): Unit = {
    import spark.implicits._

    val hrv = RandomVariable.hiddenRandomVariable("h", (0 to 2))
    val hrv1 = RandomVariable.hiddenRandomVariable("h1", (0 to 3))
    val mrv = RandomVariable.missingRandomVariable("m", (0 to 2))
    val nrv = RandomVariable.normalRandomVariable("n")

    val dataFrame = Seq(
      (1, new Integer(1)), (2, new Integer(2)),
      (0, null.asInstanceOf[Integer]), (0, new Integer(0)),
      (0, new Integer(0)), (2, null.asInstanceOf[Integer]),
      (1, new Integer(2)), (2, null.asInstanceOf[Integer]),
      (0, new Integer(0)), (1, new Integer(1)),
      (2, new Integer(2)), (0, new Integer(0))
    ).toDF(
      nrv.name,
      mrv.name
    )

    val bayesianNetwork = new BayesianNetwork(Seq(
      hrv,
      hrv1.addPi(hrv),
      mrv.addPi(hrv1).addPi(hrv),
      nrv.addPi(mrv).addPi(hrv)
    ))

    val manipulate = new Manipulate(
      actions = Seq(
        (
          Seq(
            (nrv.name, Seq(mrv.name, hrv.name)),
            (mrv.name, Seq(hrv1.name, hrv.name))
          ),
          new Manipulate(
            transforms = Seq(
              (
                Seq(
                  (hrv1.name, Seq(hrv.name)),
                  (hrv.name, Seq(hrv.name))
                ),
                hrv.name
              ),
              (
                Seq(
                  (hrv.name, Seq(hrv1.name, hrv.name)),
                  (hrv1.name, Seq(hrv1.name, hrv.name))
                ),
                hrv1.name
              )
            )
          )
        ),
        (
          Seq(
            (hrv.name, Seq(hrv.name)),
            (nrv.name, Seq(mrv.name, hrv.name)),
            (mrv.name, Seq(mrv.name, hrv1.name, hrv.name))
          ),
          new Manipulate(
            transforms = Seq(
              (
                Seq((hrv1.name, Seq(mrv.name, hrv1.name, hrv.name))),
                mrv.name
              ),
              (
                Seq((hrv1.name, Seq(mrv.name, hrv.name))),
                nrv.name
              )
            )
          )
        )
      )
    )

    def printProbability(model: Model): Unit = {
      val probability = model.toProbability(
        Seq(
          hrv.name, mrv.name,
          nrv.name, hrv1.name
        )
      ).coalesce(Seq(hrv, nrv)).
        divideByNormal(Seq(nrv.name))

      Check.flat(
        probability.dataFrame,
        probability.unNormalColumn.get.name,
        probability.unNormalColumn.get.len
      )
    }

    val filePath = "/app/map/map-client/xiehao/test/"

    val model1 = bayesianNetwork.fit(
      dataFrame = dataFrame,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug
    )

    println("model:1")
    printProbability(model1)

    model1.save(s"${filePath}1")

    val model1ByLoad = bayesianNetwork.load(spark, s"${filePath}1")

    print("modelByLoad:1")
    printProbability(model1ByLoad)

    val model2 = bayesianNetwork.fit(
      dataFrame = dataFrame,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug,
      lastModel = model1
    )

    println("model:2")
    printProbability(model2)

    model2.save(s"${filePath}2")

    val model12ByLoad = bayesianNetwork.load(spark, s"${filePath}[12]")

    print("modelByLoad:12")
    printProbability(model12ByLoad)

    val model3 = bayesianNetwork.fit(
      dataFrame = dataFrame,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug,
      lastModel = model12ByLoad
    )

    println("model:3")
    printProbability(model3)

    model3.save(s"${filePath}3")

    val model123ByLoad = bayesianNetwork.load(spark, s"${filePath}[123]")

    print("modelByLoad:123")
    printProbability(model123ByLoad)

  }

  def naive(spark: SparkSession, maxTurn: Int, debug: Boolean): Unit = {
    import spark.implicits._

    val a = RandomVariable.normalRandomVariable("a")
    val b = RandomVariable.normalRandomVariable("b")
    val c = RandomVariable.normalRandomVariable("c")
    val d = RandomVariable.normalRandomVariable("d")
    val e = RandomVariable.normalRandomVariable("e")
    val f = RandomVariable.normalRandomVariable("f")
    val g = RandomVariable.normalRandomVariable("g")

    def random = (
      Random.nextInt(2), Random.nextInt(2), Random.nextInt(2),
      Random.nextInt(2), Random.nextInt(2), Random.nextInt(2),
      Random.nextInt(3)
    )

    val dataFrame = Seq.fill(100)(random).
      toDF(a.name, b.name, c.name, d.name, e.name, f.name, g.name)

    println("DataFrame:")
    dataFrame.show()

    val bayesianNetwork = new BayesianNetwork(Seq(
      g,
      a.addPi(g), b.addPi(g), c.addPi(g),
      d.addPi(g), e.addPi(g), f.addPi(g)
    ))

    /*val manipulates = Map[String, Seq[(String, Seq[String])]]()

    val model = bayesianNetwork.fit(
      dataFrame = dataFrame,
      manipulate = manipulate,
      maxTurn = maxTurn,
      checkpointDir = checkpointDir,
      debug = debug
    )

    val probability = model.toProbability(
      Seq(
        a.name, b.name, c.name,
        d.name, e.name, f.name, g.name
      )
    )

    probability.dataFrame.cache()

    Seq(a, b, c, d, e, f).foreach(randomVariable => {
      println("Probability:")
      println(randomVariable.name)

      probability.coalesce(Seq(randomVariable, g)).
        divideByNormal(Seq(randomVariable.name)).dataFrame.show()
    })*/
  }
}

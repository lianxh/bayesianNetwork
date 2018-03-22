package me.xiehao.bayesianNetwork

import scala.math.abs
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{array, lit, struct, udf}

class UnNormalColumn(val randomVariables: Seq[RandomVariable]) {
  val names: Seq[String] = randomVariables.map(_.name)
  val name: String = UnNormalColumn.concatColumnName(names)
  val namesOfMissing: Seq[String] = randomVariables.
    filter(_.innerType == RandomVariable.MISSING).map(_.name)
  val len: Int = randomVariables.foldLeft(1){
    case (_len, randomVariable) =>
      _len * randomVariable.defaultValuesLen
  }

  val dimension: Seq[(Int, Int)] = if (randomVariables.length == 0) Seq()
    else for (i <- 0 to randomVariables.length - 1)
      yield (i, randomVariables(i).defaultValuesLen)
  val bases: Seq[(Int, Int)] = dimension.foldLeft(Seq[(Int, Int)]()){
    case (_base, (indexOfName, _)) =>
      if (indexOfName == 0) _base :+ (0, 1)
      else _base :+ (
        indexOfName,
        _base.last._2 * randomVariables(indexOfName - 1).defaultValuesLen
      )
  }
  val matrix: Seq[Seq[Int]] =
    Utility.matrix(dimension.map{case (_, len) => (0 to len - 1)})

  def ==(unNormalColumn: UnNormalColumn): Boolean =
    names.forall(name => unNormalColumn.names.contains(name))

  private def common(unNormalColumn: UnNormalColumn)
  : Seq[(Int, Int)] = {
    val commonNames: Seq[String] = Utility.intersection(
      names,
      unNormalColumn.names
    )
    for (i <- 0 to commonNames.length - 1) yield {
      (
        names.indexOf(commonNames(i)),
        unNormalColumn.names.indexOf(commonNames(i))
      )
    }
  }

  def check(dataSet: DataSet): Seq[(String, Column)] = {
    dataSet.columnNamesOfNormal.filter(name => namesOfMissing.contains(name)).
      map(nameOfMissing => (
        nameOfMissing,
        array(
          matrix.groupBy(_(names.indexOf(nameOfMissing))).map{
            case (value, rows) =>
              struct(
                array(
                  rows.map(row => lit(Utility.index(bases, row))): _*
                ).as("_1"),
                lit(value).as("_2")
              )
          }.toSeq: _*
        )
      ))
  }
  def join(unNormalColumn: UnNormalColumn)
  : (Option[UnNormalColumn], Column) = {
    val unNormalColumnAfterJoined = new UnNormalColumn(
      unNormalColumn.randomVariables.foldLeft(randomVariables){
        case (_randomVariables, randomVariable) =>
          if (_randomVariables.map(_.name).contains(randomVariable.name))
            _randomVariables
          else _randomVariables :+ randomVariable
      }
    )

    val indexesOfCommon: Seq[(Int, Int)] = common(unNormalColumn)
    val indexesOfName1: Seq[Int] = indexesOfCommon.map(_._1)
    val indexesOfName2: Seq[Int] = indexesOfCommon.map(_._2)

    val joinMap = matrix.groupBy(
      row => indexesOfName1.map(index => row(index)).mkString("-")
    ).flatMap{
      case (_, rows) => {
        val row1 = rows(0)
        unNormalColumn.matrix.filter(row2 =>
          indexesOfCommon.forall{
            case (index1, index2) => row1(index1) == row2(index2)
          }
        ).flatMap(row2 => rows.map(
          row1 => (
            row1,
            row2,
            row1 ++ row2.foldLeft((Seq[Int](), 0)){
              case ((row, index), value) =>
                (
                  if (indexesOfName2.contains(index)) row
                  else row :+ value,
                  index + 1
                )
            }._1
          )
        ))
      }
    }.map{
      case (row1, row2, row3) => (
        Utility.index(bases, row1),
        Utility.index(unNormalColumn.bases, row2),
        Utility.index(unNormalColumnAfterJoined.bases, row3)
      )
    }.toSeq

    (
      Option(unNormalColumnAfterJoined),
      array(
        joinMap.map{
          case (index1, index2, index3) =>
            struct(
              lit(index1).as("_1"),
              lit(index2).as("_2"),
              lit(index3).as("_3")
            )
        }: _*
      )
    )
  }
  def coalesce(unNormalColumn: UnNormalColumn)
  : Column = {
    val indexesOfCommon: Seq[(Int, Int)] = common(unNormalColumn)

    array(
      unNormalColumn.matrix.map(
        row2 => (
          matrix.filter(
            row1 => indexesOfCommon.forall{
              case (index1, index2) => row1(index1) == row2(index2)
            }
          ),
          row2
        )
      ).map{
        case (rows, row2) => struct(
          array(
            rows.map(row1 => lit(Utility.index(bases, row1))): _*
          ).as("_1"),
          lit(Utility.index(unNormalColumn.bases, row2)).as("_2")
        )
      }: _*
    )
  }
  def groupBy(namesToGroupBy: Seq[String]): Column = {
    val indexes = namesToGroupBy.
      map(nameToGroupBy => names.indexOf(nameToGroupBy))

    array(
      matrix.groupBy(
        row => indexes.map(index => row(index)).mkString("-")
      ).map{
        case (_, rows) =>
          array(
            rows.map(row => lit(Utility.index(bases, row))): _*
          )
      }.toSeq: _*
    )
  }
  def reset(unNormalColumn: UnNormalColumn)
  : Column = {
    val indexesOfCommon: Seq[(Int, Int)] = common(unNormalColumn)

    array(
      matrix.map(row1 => (
        row1,
        unNormalColumn.matrix.find(
          row2 => indexesOfCommon.forall{
            case (index1, index2) => row1(index1) == row2(index2)
          }
        ).get
      )).map{
        case (row1, row2) => struct(
          lit(Utility.index(bases, row1)).as("_1"),
          lit(Utility.index(unNormalColumn.bases, row2)).as("_2")
        )
      }: _*
    )
  }
}

object UnNormalColumn {
  val splitOfNames = "_"
  def concatColumnName(names: Seq[String])
  : String = names.mkString(splitOfNames)
  def splitColumnName(columnName: String)
  : Seq[String] = columnName.split(splitOfNames)

  val add = udf(
    (value: Double, values: Seq[Double]) => {
      values.map(_ + value)
    }
  )
  val multi = udf(
    (value: Double, values: Seq[Double]) => {
      values.map(_ * value)
    }
  )
  val divide = udf(
    (
      valuesOfMember: Seq[Double],
      valuesOfDenominator: Seq[Double]
    ) => {
      for (i <- 0 to valuesOfMember.length - 1) yield {
        valuesOfMember(i) / valuesOfDenominator(i)
      }
    }
  )
  val factor = udf(
    (values: Seq[Double], value: Double) =>
      values.map(_ / value)
  )
  val percent = udf(
    (values: Seq[Double]) => {
      val sum = values.sum
      values.map(_ / sum)
    }
  )
  val join = udf(
    (
      values1: Seq[Double],
      values2: Seq[Double],
      //joinMap: Seq[(Int, Int, Int)]
      joinMap: Seq[Row]
    ) => {
      joinMap.foldLeft(Vector.fill(joinMap.length)(0.0)){
        case (v, Row(index1: Int, index2: Int, index3: Int)) =>
          v.updated(
            index3,
            values1(index1) * values2(index2)
          )
      }
    }
  )
  val fractionate = udf(
    (
      values: Seq[Double],
      groupByMap: Seq[Seq[Int]]
    ) => {
      groupByMap.foldLeft(Vector.fill(values.length)(0.0)){
        case (vector, indexes) => {
          val sum = indexes.map(index => values(index)).sum

          indexes.foldLeft(vector){
            case (v, index) =>
              v.updated(index, values(index) / sum)
          }
        }
      }
    }
  )
  val check = udf(
    (
      value: Int,
      values: Seq[Double],
      //indexesMaps: Seq[(Seq[Int], Int)],
      indexesMaps: Seq[Row],
      isEqual: Boolean
    ) => {
      val vector = Vector.fill(values.length)(0.0)
      val indexesMap = indexesMaps.map(row => (
        row.getAs[Seq[Int]]("_1"),
        row.getAs[Int]("_2")
      )).find{
        case (_, _value) => _value == value
      }.get._1

      if (isEqual) {
        indexesMap.foldLeft(vector){
          case (v, index) => v.updated(index, 1.0)
        }
      } else {
        indexesMap.foldLeft(vector){
          case (v, index) => v.updated(index, values(index))
        }
      }
    }
  )
  val coalesce = udf(
    (
      values: Seq[Double],
      //indexesMaps: Seq[(Seq[Int], Int)]
      indexesMaps: Seq[Row]
    ) => {
      val vector = values.toVector
      indexesMaps.foldLeft(Vector.fill(indexesMaps.length)(0.0)){
        case (v, row) =>
          v.updated(
            row.getAs[Int]("_2"),
            row.getAs[Seq[Int]]("_1").
              map(_index => vector(_index)).sum
          )
      }
    }
  )
  val diff = udf(
    (values1: Seq[Double], values2: Seq[Double]) => {
      var sum = 0.0
      for (i <- 0 until values1.length) {
        if (values1(i) != values2(i)) {
          /*sum += (abs(values1(i) - values2(i)) /
            max(values1(i), values2(i)))*/
          sum += abs(values1(i) - values2(i))
        }
      }

      sum
    }
  )
  val all = udf(
    (values: Seq[Double]) => Seq(values.sum)
  )
  val aggregate = udf(
    (values: Seq[Double]) => values.sum
  )
  val expect = udf(
    (values: Seq[Double]) => {
      val expectation = (0 until values.length).foldLeft(0.0){
        case (_expectation, index) =>
          _expectation + (values(index) * index)
      }

      Seq(expectation, values(expectation.toInt))
    }
  )
  val reset = udf(
    (
      values: Seq[Double],
      // indexesMaps: Seq[(Int, Int)],
      indexesMaps: Seq[Row]
    ) => {
      indexesMaps.foldLeft(Vector.fill(values.length)(0.0)){
        case (v, row) => {
          v.updated(
            row.getAs[Int]("_2"),
            values(row.getAs[Int]("_1"))
          )
        }
      }
    }
  )
}

package genderclassification

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object GenderDataset {
  val gender = Map(
    "1 0 0" -> 0,
    "0 1 0" -> 1)

  def labeledDataset(userToUnitVectorCategories: RDD[(String, Iterable[Double])], userToGender: RDD[(String, String)]) = userToUnitVectorCategories
    .join(userToGender) // (User, (RelativeCategoryCount, Gender))
    .values // (RelativeCategoryCount, Gender)
    .map((x: (Iterable[Double], String)) => { // (LabeledPoint)
      val label = gender(x._2)
      val features = x._1.toArray

      new LabeledPoint(label, Vectors.dense(features))
    })
}

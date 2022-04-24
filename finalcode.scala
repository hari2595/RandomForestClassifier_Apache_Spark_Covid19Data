import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{IntegerType, DoubleType}

val df=spark.read
 .format("csv")
 .option("header","true")
 .load("hdfs://xxxx:8020/FileAdress/covid.csv")
val cleaned_DF = df.na.drop()

val dataset = cleaned_DF.select(col("Ever in ICU"),
 col("Age Group"),
 col("Source of Infection"),
 col("Outbreak Associated"),
 col("Client Gender"))


dataset.show(10)

val inputColumns = Array("Age Group","Source of Infection", "Outbreak Associated","Client Gender")
val outputColumns = Array("Age_index","Source_index","Outbreak_index","Gender_index")

val indexer = new StringIndexer()
indexer.setInputCols(inputColumns)
indexer.setOutputCols(outputColumns)

val stringIndexer = new StringIndexer()
 .setInputCol("Ever in ICU")
 .setOutputCol("ICU_index")

val DF_indexed = indexer.fit(dataset).transform(dataset)
val DF_indexed2 = stringIndexer.fit(DF_indexed).transform(DF_indexed)

val rankDf = DF_indexed2.select(col("ICU_index").cast(IntegerType),
col("Age_index").cast(IntegerType),
col("Source_index").cast(IntegerType),
col("Outbreak_index").cast(IntegerType),
col("Gender_index").cast(IntegerType))

rankDf.show(10)


val Array(trainingData, testData) = rankDf.randomSplit(Array(0.8, 0.2), 750)

val assembler = new VectorAssembler()
 .setInputCols(Array("Age_index","Source_index","Outbreak_index","Gender_index"))
 .setOutputCol("assembled-features")

val rf = new RandomForestClassifier()
 .setFeaturesCol("assembled-features")
 .setLabelCol("ICU_index")
 .setSeed(1234)

val pipeline = new Pipeline()
 .setStages(Array(assembler,rf))

val evaluator = new MulticlassClassificationEvaluator()
 .setLabelCol("ICU_index")
 .setPredictionCol("prediction")
 .setMetricName("accuracy")

val paramGrid = new ParamGridBuilder()
 .addGrid(rf.maxDepth,Array(3,4))
 .addGrid(rf.impurity, Array("entropy","gini")).build()

val cross_validator = new CrossValidator()
 .setEstimator(pipeline)
 .setEvaluator(evaluator)
 .setEstimatorParamMaps(paramGrid)
 .setNumFolds(3)

val cvModel = cross_validator.fit(trainingData)

val predictions = cvModel.transform(testData)

val accuracy = evaluator.evaluate(predictions)
println("Accuracy of the model = "+accuracy)

predictions
 .select(col("ICU_index"),
col("Age_index"),
col("Source_index"),
col("Outbreak_index"),
col("Gender_index"))
 .write
 .format("csv")
 .save("hdfs://xxxx:8020/FileAddress/")

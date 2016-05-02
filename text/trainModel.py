#!/usr/bin/spark-submit

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.classification import SVMWithSGD, SVMModel

def ramdonForest(sc):
    data = MLUtils.loadLibSVMFile(sc, 's3://anly502-yelp/format_regression_review_text_v3')
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    #  Note: Use larger numTrees in practice.
    #  Setting featureSubsetStrategy="auto" lets the algorithm choose.
    model = RandomForest.trainClassifier(trainingData, numClasses=6, categoricalFeaturesInfo={},
                                         numTrees=3, featureSubsetStrategy="auto",
                                         impurity='gini', maxDepth=4, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification forest model:')
    print(model.toDebugString())

    # Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

def Boosting(sc):
    # Load and parse the data file.
    data = MLUtils.loadLibSVMFile(sc, 's3://anly502-yelp/bin_regression_review_text_v3_v4')
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a GradientBoostedTrees model.
    #  Notes: (a) Empty categoricalFeaturesInfo indicates all features are continuous.
    #         (b) Use more iterations in practice.
    model = GradientBoostedTrees.trainClassifier(trainingData,learningRate = 0.05,maxDepth=5,maxBins=32,categoricalFeaturesInfo={}, numIterations=300)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    haha = labelsAndPredictions.collect()
    with open('predictions.txt','w') as fout:
        for v,p in haha:
            fout.write(str(v)+" "+str(p)+'\n')
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification GBT model:')
    print(model.toDebugString())

    # Save and load model
    #model.save(sc, "target/tmp/myGradientBoostingClassificationModel")
    #sameModel = GradientBoostedTreesModel.load(sc,"target/tmp/myGradientBoostingClassificationModel")

def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])

def SVM(sc):
    #data = sc.textFile("s3://anly502-yelp/bin_regression_review_text_v3")
    #parsedData = data.map(parsePoint)

    data = MLUtils.loadLibSVMFile(sc, 's3://anly502-yelp/bin_regression_review_text_v3')
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    # Build the model
    model = SVMWithSGD.train(trainingData, iterations=100)

    # Evaluating the model on test data
    predictions = model.predict(testData.map(lambda x: x.features))
    #labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    #trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
    #print("Training Error = " + str(trainErr))

    # Save and load model
    model.save(sc, "myModelPath")
    sameModel = SVMModel.load(sc, "myModelPath")

def main():
    sc     = SparkContext( appName="Train model" )
    
    #randomForest(sc)
    Boosting(sc)
    #SVM(sc)

if __name__=="__main__":
    main()

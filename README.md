# Making scala fat-jar for spark

## compile fat-jar in sbt

Run following command in project folder:
```
sbt compile
sbt package
```

Add the following line to your `project/plugins.sbt` file:
```
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
```

And finally run following command and jar file will be created.
```sbt assembly```

## run jar file by spark-submit

```
spark-submit  --master yarn --class "calculation_distance" /path_to_the_jar/calculation_distance.jar
```

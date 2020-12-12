# imdb-hadoop

Uses Apache Hadoop to list all numbers of movies release for each year and determine which year had the most releases.

## dataset

- [kaggle.com](https://www.kaggle.com/stefanoleone992/imdb-extensive-dataset)
- [movies.csv](/data_set/movies.csv)

## instructions

### create hdfs directory

```text
hadoop fs -mkdir /imdb-hadoop
hadoop fs -mkdir /imdb-hadoop/input
```

### add dataset to input folder

```text
hadoop fs -put <full_path_to_data_set_directory>\movies.csv /imdb-hadoop/input/movies.csv
```

### running project

```text
mvn clean install
cd /target
hadoop jar imdb-1.0.jar me.affanhaq.imdb.IMDB /imdb-hadoop/input/movies.csv /imdb-hadoop/output /imdb-hadoop/output2
```

### seeing the output

```text
# lists all the years and the movie count for each one
hadoop fs -cat /imdb-hadoop/output/part-r-00000

# lists the year with the most movies
hadoop fs -cat /imdb-hadoop/output2/part-r-00000
```

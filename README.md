# Map/Reduce on IMDB dataset 
This project implements Map Reduce to find all the the actors (or actresses) who also acted as directors in the same movie title

# How to run this project:

## Prerequisites
* Copy all the input files in folder "inputImdb" inside IMDB-3mapper2reducer folder 
* The input file name should be as follows:
    1.  imdb00-title-actors.csv
    2.  title.basics.tsv
    3.  title.crew.tsv
* jdk 1.8 required
* download Hadoop for [windows](https://kontext.tech/article/978/install-hadoop-332-in-wsl-on-windows) and for [Linux](https://www.edureka.co/blog/install-hadoop-single-node-hadoop-cluster)

<NOTE> setting up and running hadoop on linux is easier, if you don't have Linux try using wsl. 

## Running the project on windows-wsl2
1. I have my bashrc file present in home location. To start hadoop one node cluster, run below commands:
```
source ~/.bashrc
sudo service ssh start
sbin/start-dfs.sh
sbin/start-yarn.sh 
```

2. After setting up hadoop and all the environment variable, check if the UI of hadoop is up and running. At default node 50070 <http://localhost:50070/dfshealth.html#tab-overview>

3. Place all the input files in folder 'inputMapReduce' and Upload the folder to hadoop HDFS
```
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal <folder_location>/inputMapReduce /
```

4. Run the following command to run the project
```
$HADOOP_HOME/bin/hadoop jar <project_folder>/target/NewProjectHadoop-0.0.1-SNAPSHOT.jar com.uta.MapperReducerMain /inputMapReduce/title.basics.tsv /inputMapReduce/imdb00-title-actors.csv /inputMapReduce/title.crew.tsv /mapreduce_output
```
<NOTE>: The HDFS location may have mentioned output folder, so delete the folder first and then run above command. Folder can be deleted from HDFS using this command:
```
$HADOOP_HOME/bin/hdfs  dfs -rm -r /mapreduce_output
```

5. After completion of the command, the output folder can be seen using this command:
```
$HADOOP_HOME/bin/hdfs dfs -cat /mapreduce_output/part-r-00000
```
and to copy the files to your location, use:
```
$HADOOP_HOME/bin/hdfs dfs -get /mapreduce_output/* output-distr
```

## It is an academic project, to verify the mapper-reducer result comare it to the output of the query below on database which has tables with same dataset as in input files:
/*
	 * 
	 * SELECT * FROM imdb00.title_basics b INNER JOIN imdb00.TITLE_PRINCIPALS a ON
	 * a.TCONST=b.TCONST and a.category in ('actor','actress') and a.NCONST<>'\N'
	 * INNER join imdb00.title_crew c ON a.TCONST=c.TCONST and c.directors<> '\N'
	 * and c.directors like '%'||a.NCONST||'%' --check this as dir are comma
	 * separated Where b.Titletype='movie' AND b.startYear BETWEEN '1950' AND '1960'
	 * ORDER BY a.TCONST ASC;
	 * 
*/
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar


-- COMPILE
hadoop com.sun.tools.javac.Main ClassAverage.java

jar cf ca.jar ClassAverage*.class



-- MAKE DIR
hadoop fs -mkdir /user/tipparac/classaverage
hadoop fs -mkdir /user/tipparac/classaverage/input
hadoop fs -mkdir /user/tipparac/classaverage/output

hadoop fs -copyFromLocal /home/tipparac/DataSet /user/tipparac/classaverage/input/DataSet

hadoop fs -copyToLocal /user/tipparac/classaverage/output/part-r-00000 /home/tipparac/part-r-00000

hadoop fs -copyToLocal /user/tipparac/classaverage/output /home/tipparac/output


-- EXECUTE
hadoop jar ca.jar ClassAverage /user/tipparac/classaverage/input /user/tipparac/classaverage/output /user/tipparac/classaverage/logs 2



hadoop fs -ls /user/tipparac/classaverage/output/
hadoop fs -ls /user/tipparac/classaverage/logs/

hadoop fs -rm -r /user/tipparac/classaverage/output/

-- READ STUFF
hadoop fs -cat /user/tipparac/classaverage/output/part-r-00000

hadoop fs -cat /user/tipparac/classaverage/logs/results.txt










------------------------------------------------------------------------------------------------
-- NEW HACKER MANUAL 2018
------------------------------------------------------------------------------------------------


hadoop com.sun.tools.javac.Main CommonFriends.java ClassWritable.java DoubleCalcReducer.java TokenizerMapper.java

jar cf cf.jar CommonFriends.class ClassWritable.class DoubleCalcReducer.class TokenizerMapper.class



-- MAKE DIR
hadoop fs -mkdir /user/tipparac/commonfriends
hadoop fs -mkdir /user/tipparac/commonfriends/input
hadoop fs -mkdir /user/tipparac/commonfriends/output

hadoop fs -copyFromLocal /home/tipparac/input/testfile /user/tipparac/commonfriends/input/testfile
hadoop fs -copyFromLocal /home/tipparac/input /user/tipparac/commonfriends


-- EXTRACT FILES FOR READ
hadoop fs -copyToLocal /user/tipparac/commonfriends/output/part-r-00000 /home/tipparac/part-r-00000

hadoop fs -copyToLocal /user/tipparac/commonfriends/output /home/tipparac/output

hadoop fs -copyToLocal /user/tipparac/commonfriends/logs /home/tipparac/logs

-- Test
hadoop jar cf.jar CommonFriends /user/tipparac/commonfriends/input /user/tipparac/commonfriends/output /user/tipparac/commonfriends/logs 2


-- EXECUTE actual
hadoop jar cf.jar CommonFriends /user/tipparac/commonfriends/input /user/tipparac/commonfriends/output /user/tipparac/commonfriends/logs 2



hadoop fs -ls /user/tipparac/commonfriends/output/
hadoop fs -ls /user/tipparac/commonfriends/logs/

hadoop fs -rm -r /user/tipparac/commonfriends/output/
hadoop fs -rm -r /user/tipparac/commonfriends/input/
hadoop fs -rm -r /user/tipparac/commonfriends/logs/
-- READ STUFF
hadoop fs -cat /user/tipparac/commonfriends/output/part-r-00000

hadoop fs -cat /user/tipparac/commonfriends/logs/results.txt




-- FULL RUN
hadoop fs -rm -r /user/tipparac/commonfriends/input/
hadoop fs -rm -r /user/tipparac/commonfriends/output/

hadoop com.sun.tools.javac.Main CommonFriends.java ClassWritable.java DoubleCalcReducer.java TokenizerMapper.java

jar cf cf.jar CommonFriends.class ClassWritable.class DoubleCalcReducer.class TokenizerMapper.class

hadoop jar cf.jar CommonFriends /user/tipparac/commonfriends/input /user/tipparac/commonfriends/output /user/tipparac/commonfriends/logs 2

hadoop fs -cat /user/tipparac/commonfriends/output/part-r-00000


---log

hadoop fs -cat /user/tipparac/commonfriends/logs/results.txt
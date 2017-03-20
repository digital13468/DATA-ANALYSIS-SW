To run the jar file, please see the following.

hadoop fs -rmr <output>
hadoop fs -rmr <temprary>
hadoop jar <JAR> <CLASS> -libjars <LIBS> <INPUT> <TEMP> <OUTPUT>

arguments: <output> - the output directory path, <input> - the input directory/file path, <temporary> - the temporary path before sorting
arguments: <JAR> - the path to the JAR file, <CLASS> - the name to the CLASS, <LIBS> - the path to library

example: 
hadoop fs -rmr /scr/cchsu/lab5/exp1/output
hadoop fs -rmr /scr/cchsu/lab5/exp1/temp
hadoop jar /home/cchsu/lab_5/TopHashtags.jar TopHashtags -libjars /home/cchsu/lab_5/jsonsimple1.1.1.jar /class/s17419/lab5/university.json /scr/cchsu/lab5/exp1/temp /scr/cchsu/lab5/exp1/output/


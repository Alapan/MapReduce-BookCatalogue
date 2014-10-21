The JAR file java-json.jar is needed as an external repository. 

Input source file - A text file containing a series of author-book tuples, formatted as JSON. The file contains a subset of "https://openlibrary.org" data.

Compile commands:

1) CombineBooks.java - javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar:/home/hadoop/hadoop-1.2.1/lib/java-json.jar -d combine_books CombineBooks.java
2) QueryAuthor.java - javac -classpath /home/hadoop/hadoop-1.2.1/hadoop-core-1.2.1.jar:/home/hadoop/hadoop-1.2.1/lib/commons-cli-1.2.jar:/home/hadoop/hadoop-1.2.1/lib/java-json.jar -d query_author QueryAuthor.java

JAR file creation:

1) CombineBooks.java - jar -cvf CombineBooks.jar -C combine_books/ .
2) QueryAuthor.java - jar -cvf QueryAuthor.jar -C query_author/ .

Run commands:

1) CombineBooks.java - hadoop jar CombineBooks.jar org.hwone.CombineBooks input output
2) QueryAuthor.java - hadoop jar QueryAuthor.jar org.hwone.QueryAuthor input output J. K. Rowling


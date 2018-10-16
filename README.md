[![Maintainability](https://api.codeclimate.com/v1/badges/15f2ae226bb5a605a7af/maintainability)](https://codeclimate.com/github/JosiahMay/CS435-PA2/maintainability)

# CS435-PA2


# Programming Assignment 2

Document Summarization using TF/IDF Scores
___

### Due: October 16, 2018 By 5:00PM

### Submission: via Canvas, individual submission

# Objectives

The goal of this programming assignment is to enable you to gain experience in:
* Creating an authorship identification system based on the similarity of the author’s attributes
* Calculating TF/IDF scores using MapReduce
* Document summarization using MapReduce


## Build Project

mvn package

## Run project

### All three parts

./Run_All.sh \<Input Dir> \<Output Dir> 


### Just run the summary

$HADOOP_HOME/bin/hadoop jar ./target/PA2-1.0.jar cs435.josiahm.pa2.drivers.JobThreeDriverJoin \<Input Dir> \<Output Dir>



* \<Input Dir> is the article to read
* \<Output Dir> will have the results of the  calculations


## Run information

Job 1 first calculates the TF for each article using an identity mapper. During this job it also count all the article ID's.
After the TF values are calculated, the IDF values are calculated using map-reduce and the count from the first part.

Job 2 puts the results of IDF calculations into the hadoop cache and uses an identity mapper read the results of the TF
calculations. It outputs the results of the IDF*TF for every TF value

Job 3 uses a reduce side join to read the articles and the IDF\*TF calculations. The reducer gets all the sentences of the 
article and the IDF\*TF values for each word. The reducer then finds the top 5 words in each sentence their IDF\*TF and scores
each sentence by the sum of the five words. The reducer then finds the top 3 sentences and writes a summary of the sentences in
the order they were in the article.

## Times for complete data set

* Job 1/2 5:10
* Job 3 3:50 - 5:20
* Total 8:30 - 9:32

## Issues in the assignment

* Attempts to find the summary using distributed cache ran into GC errors, not enough JVM memory
* Creating the inputFormats, ran into issues with changes from hadoop 2.x and 3.x
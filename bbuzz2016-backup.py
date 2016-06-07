# Databricks notebook source exported at Tue, 7 Jun 2016 15:36:32 UTC
import re
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, StringType

# COMMAND ----------

MY_AWS_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY_HERE"
MY_AWS_SECRET_KEY = "YOUR_AWS_SECRET_HERE"

# COMMAND ----------

import urllib
ACCESS_KEY = MY_AWS_ACCESS_KEY
SECRET_KEY = MY_AWS_SECRET_KEY
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "bbuzz2016" # AWS_BUCKET_NAME = "bbuzz2016data" 
MOUNT_NAME = "bbuzz2016" # MOUNT_NAME = "bbuzz2016data"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bbuzz2016"))

# COMMAND ----------

bbuzz = sqlContext.read.json("dbfs:/mnt/bbuzz2016/all-years.jsonlines")
bbuzz.registerTempTable("bbuzz_raw")

# COMMAND ----------

display(sqlContext.sql("SELECT * FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

display(sql("SELECT lower(content) FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

def cleanString(x):
    return re.sub('\s+', ' ', re.sub('[^a-zA-Z0-9 ]', '', x)).strip()
sqlContext.registerFunction('cleanString', cleanString)

display(sql("SELECT cleanString(lower(content)) FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

stopwords = set(sc.textFile("dbfs:/mnt/bbuzz2016/stopwords.txt").collect())
def removeStopwords(x):
    return ' '.join([word for word in x.split(' ') if word not in stopwords])
sqlContext.registerFunction('removeStopwords', removeStopwords)
#stopwords
#display(sqlContext.sql("SELECT removeStopwords(cleanString(lower(content))) FROM bbuzz_raw WHERE content <> '' LIMIT 10"))
# -> See https://issues.apache.org/jira/browse/SPARK-11159

sqlContext.registerFunction('clearAll', lambda x: removeStopwords(cleanString(x)))
display(sql("SELECT clearAll(lower(content)) FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

# Cooler: http://www.nltk.org/index.html
#wnl = WordNetLemmatizer()
#wnl.lemmatize(x)
# Alternatives:
# * http://nlp.stanford.edu/software/
# * https://opennlp.apache.org/ 
def lemmatize(x):
    return ' '.join([re.sub('s$', '', word) for word in x.split(' ')])
sqlContext.registerFunction('lemmatize', lemmatize)

sqlContext.registerFunction('clearAll', lambda x: lemmatize(removeStopwords(cleanString(x))))
display(sql("SELECT clearAll(lower(content)) FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

pairs = [
  'big data',
  'open source',
  'machine learning',
  'real time',
  'berlin buzzword',
  'search engine',
  'data store',
  'event sourcing',
  'map reduce'
]
def tokenize(x):
    for pair in pairs:
        x = x.replace(pair, pair.replace(' ', ''))
    return x
sqlContext.registerFunction('tokenize', tokenize)

sqlContext.registerFunction('clearAll', lambda x: tokenize(lemmatize(removeStopwords(cleanString(x)))))
display(sql("SELECT clearAll(lower(content)) FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

display(sql("SELECT link FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

display(sql("SELECT regexp_extract(regexp_replace(link, 'www\.berlinbuzzwords\.de', '2016.berlinbuzzwords.de'), '([0-9]+)\.berlinbuzzwords', 1), link FROM bbuzz_raw WHERE content <> '' LIMIT 10"))

# COMMAND ----------

bbuzz_websites = sqlContext.sql("""
SELECT
  link,
  speakers,
  regexp_extract(regexp_replace(link, 'www\.berlinbuzzwords\.de', '2016.berlinbuzzwords.de'), '([0-9]+)\.berlinbuzzwords', 1) as year,
  cleanString(lower(title)) as title,
  cleanString(lower(content)) as content,
  clearAll(lower(concat(title, ' ', content))) as body
FROM bbuzz_raw
""")
bbuzz_websites.registerTempTable("bbuzz_websites")
display(sql("SELECT * FROM bbuzz_websites LIMIT 10"))

# COMMAND ----------

sessions = sqlContext.sql("""
  SELECT * FROM bbuzz_websites
  WHERE
    (link LIKE '%.de/content%' OR link LIKE '%.de/session%')
    AND NOT (title LIKE 'lunch%' OR title LIKE 'coffee break%' OR title LIKE 'lightning talk%' OR title LIKE '% hackathon' OR title LIKE '% workshop' OR title LIKE '% barcamp')
""")
sessions.registerTempTable("sessions")
display(sql("SELECT link, title FROM sessions WHERE year = 2015"))

# COMMAND ----------

display(sessions.groupBy("year").count().orderBy("year"))

# COMMAND ----------

display(sqlContext.createDataFrame(
    sessions.select("speakers")
            .flatMap(lambda line: line.speakers)
            .map(lambda x: (x, 1))
            .reduceByKey(lambda a, b: a + b)
            .map(lambda x: Row(speaker=x[0], talks=x[1]))
).orderBy('talks', ascending=False))

# COMMAND ----------

counts = sqlContext.createDataFrame(
    sessions.select("body") \
            .flatMap(lambda line: line.body.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .map(lambda a: Row(word=a[0], count=a[1])) \
)
counts.registerTempTable("wordcounts")
top_counts = sqlContext.sql("SELECT word, count FROM wordcounts ORDER BY count DESC LIMIT 50")
display(top_counts)

# COMMAND ----------

counts = sqlContext.createDataFrame(
    sessions.select('year', 'body') \
            .flatMap(lambda line: [(line.year, word) for word in line.body.split(" ")]) \
            .map(lambda line: ((line[0], line[1]), 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .map(lambda a: Row(year=a[0][0], word=a[0][1], count=a[1])) \
)
counts.registerTempTable("wordcounts_per_year")
top_counts = sqlContext.sql("SELECT year, word, count FROM wordcounts_per_year ORDER BY count DESC LIMIT 50")
display(top_counts)

# COMMAND ----------

def timeline(terms):
    return sqlContext.sql("""
SELECT year, word, count FROM wordcounts_per_year
WHERE word IN ('%s')
ORDER BY year, count ASC
""" % ("', '".join(terms)))

def timelineRelative(terms):
    return sqlContext.sql("""
SELECT w.year, word, count, count / total * 100 as relative_count FROM wordcounts_per_year w LEFT JOIN (SELECT year, sum(count) as total FROM wordcounts_per_year GROUP BY year) t ON (w.year = t.year)
WHERE word IN ('%s')
ORDER BY year, count ASC
""" % ("', '".join(terms))) 

# COMMAND ----------

display(timeline([
  'lucene',
  'solr',
  'elasticsearch',
]))   

# COMMAND ----------

display(timelineRelative([
  'lucene',
  'solr',
  'elasticsearch',
]))   

# COMMAND ----------

display(timelineRelative([
  'storm',
  'flink',
  'spark',
]))

# COMMAND ----------

display(timelineRelative([
  'hadoop',
  'mapreduce',
  'hdf',
  'yarn',
  'nosql',
  'sql',
]))  

# COMMAND ----------

display(timelineRelative([
  'nosql',
  'sql',
  'graph',
]))   

# COMMAND ----------

display(timelineRelative([
  'cassandra',
  'hbase',
  'redi',
  'riak',
  'couchdb',
  'mongodb',
]))   

# COMMAND ----------

display(timelineRelative([
  'streaming',
  'realtime',
  'batch',
]))   

# COMMAND ----------

display(timelineRelative([
  'hive',
  'pig',
  'impala',
  'presto',
  'sparksql',
  'drill',
]))   

# COMMAND ----------

display(timelineRelative([
  'crunch',
  'eventsourcing',
  'blockchain',
]))   

# COMMAND ----------

old = ['hadoop', 'mapreduce', 'hdf']
new = ['spark', 'flink', 'storm']

display(sqlContext.sql("""
SELECT w.year, CASE WHEN word in ('%s') THEN 'hadoop+mapreduce+hdfs' ELSE 'spark+flink+storm' END AS technology, count, count / total * 100 as relative_count
FROM wordcounts_per_year w LEFT JOIN (SELECT year, sum(count) as total FROM wordcounts_per_year GROUP BY year) t ON (w.year = t.year)
WHERE word IN ('%s')
ORDER BY year, count ASC
""" % ("', '".join(old), "', '".join(old + new))))

# COMMAND ----------



# COMMAND ----------

def stripArray(x):
    return [element.lower().strip() for element in x]
sqlContext.registerFunction("stripArray", stripArray, ArrayType(StringType()))

# COMMAND ----------

display(sqlContext.sql("SELECT title, body FROM bbuzz_websites WHERE year = 2016 and (link LIKE '%/content%' OR link LIKE '%/session%') LIMIT 50"))

# COMMAND ----------

display(sqlContext.sql("SELECT year, count(distinct link) as urls, sum(length(body)) as text_length, sum(length(body)) / count(distinct link) avg_document_size FROM bbuzz_websites GROUP BY year ORDER BY year"))

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/cs100/lab3/data-001/'))

# COMMAND ----------

display(sqlContext.sql("SELECT year, link, body FROM bbuzz_websites WHERE year = 2010 LIMIT 100"))

# COMMAND ----------

display(sqlContext.sql("SELECT year, count(distinct link) as urls, sum(length(body)) as text_length, sum(length(body)) / count(distinct link) avg_document_size FROM bbuzz_websites WHERE (link LIKE '%/content%' OR link LIKE '%/session%') GROUP BY year ORDER BY year"))

# COMMAND ----------

display(sqlContext.sql("SELECT year, link, speakers, title FROM sessions WHERE year = 2012 ORDER BY title"))

# COMMAND ----------

sqlContext.registerFunction("array_size", lambda x: len(x))
display(sqlContext.sql("SELECT year, sum(array_size(split(body, ' '))) as number_of_words FROM sessions GROUP BY year ORDER BY year"))

# COMMAND ----------

wordcounts = sqlContext.createDataFrame(sessions.select("body") \
        .flatMap(lambda line: line.body.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda a: Row(word=a[0], count=a[1]))) \
        .sort("count", ascending=False)


#wordcounts.select("word").rdd.map(lambda x: x[0]).collect()

#http://stackoverflow.com/questions/34423281/spark-dataframe-word-count-per-document-single-row-per-document

#display(sqlContext.createDataFrame(sessions.select("year").distinct().map(wordcount)))

# COMMAND ----------

counts = sqlContext.createDataFrame(
    sessions.select("body") \
            .flatMap(lambda line: line.body.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .map(lambda a: Row(word=a[0], count=a[1])) \
)
counts.registerTempTable("wordcounts")
top_counts = sqlContext.sql("SELECT word, count FROM wordcounts ORDER BY count DESC LIMIT 50")
display(top_counts)

# COMMAND ----------

top_counts = sqlContext.sql("SELECT word, count FROM wordcounts ORDER BY count DESC LIMIT 50")
display(top_counts)

# COMMAND ----------

counts = sqlContext.createDataFrame(
    sessions.select("speakers") \
            .flatMap(lambda line: line.speakers) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .map(lambda a: Row(word=a[0], count=a[1])) \
)
counts.registerTempTable("top_speakers")
top_speakers = sqlContext.sql("SELECT word, count FROM top_speakers ORDER BY count DESC LIMIT 50")
display(top_speakers)

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.feature import HashingTF as MllibHashingTF, IDF as MllibIDF
from pyspark.mllib.linalg import SparseVector

# COMMAND ----------

def run_tf_idf_spark_mllib(df, numFeatures=1 << 20):
    tokenizer = Tokenizer(inputCol="body", outputCol="words")
    wordsData = tokenizer.transform(df)

    words = wordsData.select("words").rdd.map(lambda x: x.words)

    hashingTF = MllibHashingTF(numFeatures)
    tf = hashingTF.transform(words)
    tf.cache()

    idf = MllibIDF().fit(tf)
    tfidf = idf.transform(tf)

    # @TODO make this nicer
    tmp = sqlContext.createDataFrame(wordsData.rdd.zip(tfidf), ["data", "features"])
    tmp.registerTempTable("tmp")
    old_columns = ', '.join(map(lambda x: 'data.%s' % x, wordsData.columns))
    with_features = sqlContext.sql("SELECT %s, features FROM tmp" % old_columns)
    tmp = sqlContext.createDataFrame(with_features.rdd.zip(tf), ["data", "rawFeatures"])
    tmp.registerTempTable("tmp")
    old_columns = ', '.join(map(lambda x: 'data.%s' % x, with_features.columns))
    return sqlContext.sql("SELECT %s, rawFeatures FROM tmp" % old_columns)

# COMMAND ----------

def run_tf_idf_spark_ml(df, numFeatures=1 << 20):
    tokenizer = Tokenizer(inputCol="body", outputCol="words")
    wordsData = tokenizer.transform(df)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=numFeatures)
    featurizedData = hashingTF.transform(wordsData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)

    return idfModel.transform(featurizedData)

# COMMAND ----------

def idf_scores_for_terms(tfidf, group_key):

    group_keys = tfidf.select(group_key).collect()
    words = tfidf.select("words").collect()
    features = tfidf.select("features").collect()
    rawFeatures = tfidf.select("rawFeatures").collect()
    
    numFeatures = features[0].features.size
    mllibHashingTf = MllibHashingTF(numFeatures=numFeatures)
    
    idfscores = list()
    for line_idx, feature_row in enumerate(features):
        terms = dict()

        for term_array in words[line_idx]:
            for term in term_array:
                term_index = mllibHashingTf.indexOf(term)
                terms[term] = (feature_row.features[term_index], rawFeatures[line_idx].rawFeatures[term_index])
        for term, score in terms.iteritems():
            idfscores.append((group_keys[line_idx][group_key], term, float(score[0]), float(score[1])))

    return idfscores

# COMMAND ----------

tfidf = run_tf_idf_spark_mllib(sessions)
scores = sqlContext.createDataFrame(sc.parallelize(idf_scores_for_terms(tfidf, "link")), ["link", "term", "score", "tf"])
scores.registerTempTable("scores")

# COMMAND ----------

top_terms = sqlContext.sql("SELECT term, score, tf, link FROM scores WHERE link = 'https://www.berlinbuzzwords.de/session/live-hack-analyzing-7-years-buzzwords-scale' ORDER BY score DESC")
display(top_terms)

# COMMAND ----------

top_terms = sqlContext.sql("SELECT term, score, tf, link FROM scores WHERE link = 'http://2015.berlinbuzzwords.de/session/machine-learning-startup-big-data-company' ORDER BY score DESC")
display(top_terms)

# COMMAND ----------

def concat(a, b):
  return '%s %s' % (str(a), str(b))
sessions_per_year = sqlContext.createDataFrame(sessions.select("year", "body").rdd.combineByKey(str, concat, concat).collect(), ['year', 'body'])
sessions_per_year.registerTempTable("sessions_per_year")
display(sessions_per_year)

# COMMAND ----------

tfidf_per_year = run_tf_idf_spark_mllib(sessions_per_year)
#display(tfidf_per_year.select("features"))
scores_per_year = sqlContext.createDataFrame(sc.parallelize(idf_scores_for_terms(tfidf_per_year, "year")), ["year", "term", "score", "tf"])
scores_per_year.registerTempTable("scores_per_year")

# COMMAND ----------

top_terms_per_year = sqlContext.sql("""
SELECT year, term, score, rank FROM (
  SELECT year, term, score, dense_rank() OVER (PARTITION BY year ORDER BY score DESC) as rank
  FROM scores_per_year
  WHERE score <> 0
) tmp
WHERE
  rank <= 10
ORDER BY year, rank ASC
""")
display(top_terms_per_year)

# COMMAND ----------

# Prove that hashing functions are different in mllib and ml
mllibHashingTf = MllibHashingTF(numFeatures=numFeatures)

def indexOf(term):
    """ Returns the index of the input term. """
    return hash(term) % numFeatures

words = first[0].words
features = first[0].rawFeatures
for index, term in enumerate(words):
    term_index = indexOf(term)
    print '%s %s %d %f %s' % (index, term, term_index, features[term_index], term_index in features.indices)

print words
print features

# COMMAND ----------

def timeline(terms):
    return sqlContext.sql("""
SELECT year, term, tf FROM scores_per_year
WHERE term IN ('%s')
ORDER BY year, tf ASC
""" % ("', '".join(terms))) 

# COMMAND ----------

def timelineRelative(terms):
    return sqlContext.sql("""
SELECT s.year, term, tf, total, tf / total as relative_tf FROM scores_per_year s LEFT JOIN (SELECT year, sum(tf) as total FROM scores_per_year GROUP BY year) t ON (s.year = t.year)
WHERE term IN ('%s')
ORDER BY year, tf ASC
""" % ("', '".join(terms))) 

# COMMAND ----------

display(timelineRelative([
  'lucene',
  'solr',
  'elasticsearch',
]))   

# COMMAND ----------

display(timeline([
  'lucene',
  'solr',
  'elasticsearch',
]))   

# COMMAND ----------

display(timeline([
  'storm',
  'flink',
  'spark',
]))

# COMMAND ----------

display(timeline([
  'mapreduce',
  'hdf',
  'yarn',
  'nosql',
  'sql',
]))   

# COMMAND ----------

display(timeline([
  'nosql',
  'sql',
  'graph',
]))   

# COMMAND ----------

display(timeline([
  'cassandra',
  'hbase',
  'redi',
  'riak',
  'couchdb',
  'mongodb',
]))

# COMMAND ----------

display(timeline([
  'streaming',
  'realtime',
  'batch',
]))

# COMMAND ----------

display(timeline([
  'hive',
  'pig',
  'impala',
  'presto',
  'sparksql',
  'drill',
]))

# COMMAND ----------

display(timeline([
  'crunch',
  'eventsourcing',
]))

# COMMAND ----------

old = ['hadoop', 'mapreduce', 'hdf']
new = ['spark', 'flink', 'storm']

display(sqlContext.sql("""
SELECT year, CASE WHEN term in ('%s') THEN 'hadoop+mapreduce+hdfs' ELSE 'spark+flink+storm' END AS technology, tf FROM scores_per_year
WHERE term IN ('%s')
ORDER BY year, tf ASC
""" % ("', '".join(old), "', '".join(old + new))))

# COMMAND ----------

display(sqlContext.sql("""
SELECT year, sum(tf) FROM scores_per_year
GROUP BY year
ORDER BY year ASC
"""))

# COMMAND ----------

display(sqlContext.sql("""
SELECT term, tf FROM scores_per_year
WHERE year = 2016
ORDER BY tf DESC LIMIT 20
"""))

# COMMAND ----------



# COMMAND ----------

dbutils.fs.cp('/databricks-datasets/cs100/lab3/data-001/stopwords.txt', '/mnt/bbuzz2016/stopwords.txt')

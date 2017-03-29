#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ./bin/spark-submit --executor-memory 12G /Users/mwang/Rbenchmark/R/SQLOperation.R
library(SparkR)
library(tictoc)

# Initialize SparkSession
sparkR.session(appName = "SparkR-SQL-Benchmark")

# Create a DataFrame from a JSON file
path <- "/Users/mwang/Rbenchmark/data/tweetsMixSample.json" 
df <- read.json(path)

# Register this DataFrame as a table.
createOrReplaceTempView(df, "tweets")

# Select English Tweets 
tweets <- sql("SELECT * FROM tweets WHERE lang == 'en'") 

# Call collect to get a local data.frame
tictoc::tic()
# tweetsLocalDF <- collect(tweets)
tweetsLocalDF <- take(tweets, 20000)

# Print the tweets in our dataset
print(tweetsLocalDF)
tictoc::toc()

# Stop the SparkSession now
sparkR.session.stop()

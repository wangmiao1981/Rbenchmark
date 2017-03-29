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
tweetsLocalDF <- take(tweets, 10000)

# Print the tweets in our dataset
print(tweetsLocalDF)
tictoc::toc()

# Stop the SparkSession now
sparkR.session.stop()

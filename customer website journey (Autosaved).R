
#############################################
#### Try to connect R directly to the warehouse but it's having problem with downloading the RJBDC package 
#     due to rJava 

#install.packages("RJDBC")
#install.packages("rJava")
#library(RJDBC)
#library(rJava)


#download.file('https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.37.1061/RedshiftJDBC42-no-awssdk-1.2.37.1061.jar','RedshiftJDBC42-no-awssdk-1.2.36.1060.jar')

# connect to Amazon Redshift
#driver <- RJDBC::JDBC("com.amazon.redshift.jdbc.Driver", "RedshiftJDBC42-no-awssdk-1.2.36.1060.jar", identifier.quote="`")

# url <- "<JDBCURL>:<PORT>/<DBNAME>?user=<USER>&password=<PW>
#url <- "jdbc:postgresql://prod-analyticsetl-analyticsetlredshiftcluster-4pslag0smnmk.cpjmk8cgf3f9.us-east-1.redshift.amazonaws.com:5439/analyticsetlprod?tcpKeepAlive=true"
#conn <- dbConnect(driver, url)
#############################################

# Set the directory to the dataset folder
setwd("/Users/anhphan/datasets")

install.packages("sunburstR")
library(sunburstR)
# read in sample visit-sequences.csv data provided in source
# https://gist.github.com/kerryrodden/7090426#file-visit-sequences-csv

# Jan 2019 to Nov 2019 data
website_data_jan_nov_2019 <- read.csv( 
		"website_journey_jan_nov_2019.csv",
		 header=T ,
		 stringsAsFactors = FALSE
		 ) 
		 
# view top 5 rows
head(website_data_jan_nov_2019,5)	

vars <- c("full_journey_path", "total_sessions")
sequence_data_full <- website_data_jan_nov_2019[vars]

sunburst(sequence_data_full)


# Nov 2019 data
website_data_nov_2019 <- read.csv( 
		"website_journey_nov_2019.csv",
		 header=T ,
		 stringsAsFactors = FALSE
		 ) 
		 

vars <- c("full_journey_path", "total_sessions")
sequence_data_nov_2019 <- website_data_nov_2019[vars]

sunburst(sequence_data_nov_2019)





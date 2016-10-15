# Data-Visualization

# Install the following software

1. [Visual Studio 2015 CE](https://go.microsoft.com/fwlink/?LinkId=691978&clcid=0x409)
1. [MySQL Community Server 5.7.15](https://dev.mysql.com/downloads/mysql/)
1. [MySQL Workbench 6.3](http://www.mysql.com/products/workbench/)
1. [Dotnet Core](https://www.microsoft.com/net/core#windows)

# Setup Database

1. Open MySQL Workbench
1. Connect to your instance of MySQL
1. Create a new user `yelp_etl`, with same password as root. Give this user DBA permissions. Follow [instructions](https://dev.mysql.com/doc/workbench/en/wb-mysql-connections-navigator-management-users-and-privileges.html)

# Prepare to run

1. Download the dataset from [Yelp]()
1. Copy the following files into `Yelp Challenge Data Loader/data`
  * _yelp_academic_dataset_business.json_
  * _yelp_academic_dataset_checkin.json_
  * _yelp_academic_dataset_review.json_
  * _yelp_academic_dataset_tip.json_
  * _yelp_academic_dataset_user.json_

# Run

1. Using Windows Command Prompt, navigate to `/YelpDataChallenge`
1. Run the following commands in this order:
  1. `dotnet restore`
  1. `dotnet build`
  1. `dotnet run`

This step should take a while...

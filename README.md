# Data-Visualization

# Install the following software

1. [Visual Studio 2015 CE](https://go.microsoft.com/fwlink/?LinkId=691978&clcid=0x409)
1. [MySQL Community Server 5.7.15](https://dev.mysql.com/downloads/mysql/)
1. [MySQL Workbench 6.3](http://www.mysql.com/products/workbench/)
1. [Dotnet Core](https://www.microsoft.com/net/core#windows)

# Setup Database

1. Open MySQL Workbench
1. Connect to your instance of MySQL
1. Perform a data import with `/data/yelp_db.sql`. Make sure to select *Import from Self-Contained File*. [See documentation](https://dev.mysql.com/doc/workbench/en/wb-admin-export-import-management.html)

# Run

1. Use Windows Command Prompt to navigate to the direcotry you downloaded this project to
1. Run the following commands in this order:
  1. `dotnet restore`
  1. `dotnet build`
  1. `dotnet run`

This step should take a while...

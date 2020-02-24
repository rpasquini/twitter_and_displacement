# Twitter and Displacement

This is a work in progress project that implements predictive analysis of gentrification and displacement measures on the basis of geo-tagged tweets based metrics.  
For this purpose a number of metrics are designed and implemented, including:
1. Twitter users home-location and work-location analysis.
2. Aggregation of twitter activity in the territory by residents and non-residents.
3. Dynamic (time-series style) analysis of tweet activity to detect changes in time.
 
The project handles large databases of tweets (it is being tested on 20 million tweets), and it is being implemented with technology suitable for further scalability.


# Dependencies and Installation

The project is written in Python 3.6.
Uses MongoDB as a database. A few functions are available for initial population of tweets.  
For Spatial analysis tasks this project uses [GeoPandas](https://geopandas.org/) and the [H3 library](https://github.com/uber/h3).

Dependencies are listed in the environment.yml file and can be easily installed using [Conda](https://docs.conda.io/en/latest/miniconda.html).

```
 conda env create -f environment.yml
 ```

Windows users might face problems installing H3. To avoid problems, I suggest using a Linux subsystem. 
A complete explanation for such an installation is available [here](https://ricardopasquini.com/installing-h3-on-windows-10/) 


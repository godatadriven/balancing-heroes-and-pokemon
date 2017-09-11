[![Build Status](https://travis-ci.org/godatadriven/berlin-buzzwords-balancing-heroes-and-pokemon.svg?branch=master)](https://travis-ci.org/godatadriven/berlin-buzzwords-balancing-heroes-and-pokemon)

# Balancing Heroes and Pokemon

Balancing Heroes and Pokemon in Real Time: A Streaming Variant of Trueskill for Online Ranking 

We demonstrate a matchmaking system for online video games that needs to work in a streaming setting. In particular we will demonstrate a solution to the following problems; 

- How can you estimate the skill of a video game player in an online setting? Note that this needs to work for one vs. one player games as well as games with a team setting. 

- Given these skill estimations, how can you match them such that each player is always playing against a similar skill level and doesn't need to wait very long. Note that this needs to work in a distributed session as well. 

To demonstrate an easy setting we will demonstrate how we are able to rank pokemon in one vs. one matches. To demonstrate a harder setting we will streaming game logs from heroes of the storm into our algorithm to show how it works. We intend to demonstrate a solution to this problem both on an engineering perspective (mainly handled by Fokko) as well as a machine learning perspective (mainly handled by Vincent). 

The skill estimation algorithm can in part be found described on Vincent's blog: http://koaning.io/pokemon-recommendations-part-2.html

## How to run the application

We need to run in cluster mode because we used Flink's queryable state.

```
export FLINK_TM_HEAP=4g
export FLINK_HOME=/Users/fokkodriesprong/Downloads/flink-1.3.2
export APP_HOME=`pwd`
$FLINK_HOME/bin/flink run $APP_HOME/target/scala-2.11/tru-scale.jar
```

You might want to tune the `parallelism.default: 8` config in your Flink config. This will heat up your Macbook so your coffee won't get cold. Set the number to the number of logical cores of your machine.

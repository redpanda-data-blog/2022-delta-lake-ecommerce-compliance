# Achieving compliant eCommerce data streaming with Delta Lake and Redpanda

Learn how to integrate Delta Lake with Redpanda using Spark Streaming in a data-compliant eCommerce use case scenario.

Follow along with [this tutorial on the Redpanda blog](https://redpanda.com/blog/delta-lake-spark-streaming-data-compliance) to put this demo into action. 

---------------------

# Code to integrate a Redpanda topic with Delta Lake

### Topic schema

![image](https://user-images.githubusercontent.com/102608342/162590041-828268d2-5cf3-49e1-a56e-cc642cc6ad62.png)

### Steps to get the code up and running
1) Clone the workspace
2) cd to the directory when you have cloned the codebase
3) ```sudo apt-get update```
4) ```sudo apt-get install python3-pip python3-dev python3-virtualenv```
5) ```virtualenv myprojectenv```
6) ```source myprojectenv/bin/activate```
7) ```pip3 install -r requirements.txt```

### Update .env values
The default assumption in this workspace is that we will be using a single node Redpanda cluster or localhost setup of Redpanda.
So if you have setup Redpanda in a different manner then you will need to update the ```REDPANDA_BROKER_IP``` value
in the .env file

![image](https://user-images.githubusercontent.com/102608342/162590098-45ddcac8-87ef-46b0-ab03-22d9906cc0e4.png)

The path in which the delta lake table will be created is also configurable. In this codebase by default we keep in 
```/tmp/delta/clickstream_table```. If you want to change the directory you can update the following the .env variables.

![image](https://user-images.githubusercontent.com/102608342/162590165-9a3b6bc7-27cf-47eb-87bb-1891e156fad1.png)

### To generate the stream of data based on the schema above

```python redpanda_producer.py```

### To load entries to Delta Lake. (Ensure that the producer is running prior to this). 

```python load_data.py```

Node: There is a sleep in this code so that streams get continuously written to Delta Lake.

### To view your data from Delta Lake. This command should be run in a seperate terminal. (The first terminal should have load_data.py running in it)

```python view_data.py```

-----------------------

## About Redpanda 

Redpanda is Apache Kafka® API-compatible. Any client that works with Kafka will work with Redpanda, but we have tested the ones listed [here](https://docs.redpanda.com/docs/reference/faq/#what-clients-do-you-recommend-to-use-with-redpanda).

* You can find our main project repo here: [Redpanda](https://github.com/redpanda-data/redpanda)
* Join the [Redpanda Community on Slack](https://redpanda.com/slack)
* [Sign up for Redpanda University](https://university.redpanda.com/) for free courses on data streaming and working with Redpanda


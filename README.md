# Code to integrate a redpanda topic with delta lake

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
The default assumption in this workspace is that we will be using a single node redpanda cluster or localhost setup of redpanda.
So if you have setup redpanda in a different manner then you will need to update the ```REDPANDA_BROKER_IP``` value
in the .env file

![image](https://user-images.githubusercontent.com/102608342/162590098-45ddcac8-87ef-46b0-ab03-22d9906cc0e4.png)

The path in which the delta lake table will be created is also configurable. In this codebase by default we keep in 
```/tmp/delta/clickstream_table```. If you want to change the directory you can update the following the .env variables.

![image](https://user-images.githubusercontent.com/102608342/162590165-9a3b6bc7-27cf-47eb-87bb-1891e156fad1.png)

### To generate the stream of data based on the schema above

```python redpanda_producer.py```

### To load entries to delta lake. (Ensure that the producer is running prior to this). 

```python load_data.py```

Node: There is a sleep in this code so that streams get continuously written to delta lake.

### To view your data from delta lake. This command should be run in a seperate terminal. (The first terminal should have load_data.py running in it)

```python view_data.py```


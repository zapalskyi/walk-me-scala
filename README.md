## Spark Interview Exercise

### Prerequisites

Install spark notebook locally.
For Scala use Zeppelin notebook:
sudo docker run -p 8080:8080 --rm --name zeppelin apache/zeppelin:0.8.1
For Python use Jupyter notebook:
sudo docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook

### Overview

In this exercise, you’re asked to write a job that anonymizes personal data in logs of page views and clicks, and creates a backup
as two files with all the data de-normalized and sorted by the chronological order of the events. Visitor views a page and then
may click on one of the elements on the page. Two data directories are provided — one with ‘views’ and another with ‘clicks’.
To be compliant with the security requirements you need to: 1. Mask the last octet of the IP. In this exercise assume that visitor’s
IP is not changing. 2. Hash the email with md5 and salt string. In this exercise use a static variable with value “salt” as a salt.
This exact result should appear in two files in directory ./backup.

File one:
```
timestamp,action,hashed_email,masked_ip
1533081600,view,E9F185D476235DA1984610D15C5CC050,24.19.135.0
1533081608,click-icon,E9F185D476235DA1984610D15C5CC050,24.19.135.0
1533081700,view,18678774397830E10E7420DFFF73EEC0,24.19.135.0
1533081710,click-bubble,18678774397830E10E7420DFFF73EEC0,24.19.135.0
```

File two:
```
timestamp,action,hashed_email,masked_ip
1533081800,view,6AA69FF33681223B0267052064457FAD,51.38.134.0
1533081815,click-cubic,6AA69FF33681223B0267052064457FAD,51.38.134.0
1533081900,view,01D47DC68992CF99A2BF7245FE260230,73.231.107.0
```

Create data files in

./views/data.csv
```
timestamp,email,ip
1533081800,avi.h@example.com,51.38.134.189
1533081600,daniel.a@example.com,24.19.135.244
1533081900,dudi.j@example.com,73.231.107.237
1533081700,moshe.d@example.com,24.19.135.234
```

./clicks/data.csv
```
ts,element,email
1533081815,cubic,avi.h@example.com
1533081608,icon,daniel.a@example.com
1533081710,bubble,moshe.d@example.com
```

When you’re finished writing the code send it back in a text file. Please make sure you followed all the requirements before
sending the code. Good luck!

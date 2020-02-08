# cdc-yb-issue

Repository reproducing multiple changes linked to incorrect primary keys in YB CDC,
and missing CDC records.

Note that the lib/org.yb.cdc jar is from: https://github.com/yugabyte/maven-packages/packages
## Installation

A jdk - OpenJDK 8 or above should work

Optionally: [Leiningen](https://github.com/technomancy/leiningen).
 This repository includes a script that will download leiningen,
conveniently named lein from https://github.com/technomancy/leiningen.

A [Yugabyte-DB cluster](https://github.com/yugabyte/yugabyte-db)
  * A helm chart for deploying to k8s can be found at: https://github.com/yugabyte/charts/tree/master/stable/yugabyte
## Usage
```
    $ YB_TSERVER=yb-tservers <or your tserver hostname> java -jar target/uberjar/cdc-yb-issue-0.1.0-SNAPSHOT-standalone.jar
    $ java -jar yb-cdc-connector.jar --table_name cdc_yb.issue_replication --master_addrs yb-masters --log_only
```

In the output of the second command, you will see change records like:

```
2020-02-07 19:16:05,621 [INFO|org.yb.cdc.LogClient|LogClient] time: 6476271313146322944
operation: WRITE
key {
  key: "id"
  value {
    string_value: "d1230bd9-ccc7-42b9-9874-12dd9c875579"
  }
}
key {
  key: "key"
  value {
    string_value: "name"
  }
}
changes {
  key: "value"
  value {
    string_value: "Karen"
  }
}
changes {
  key: "value"
  value {
    string_value: "XeLpx33Y0E8U@eKY08kd30XVKkc07QH297SJw.SVbnnwqn6"
  }
}
changes {
  key: "value"
  value {
    string_value: "https://90"
  }
}
```

Only `Karen` is correct, as can be seen by querying the table:

```
cqlsh> select * from cdc_yb.issue_replication WHERE id='d1230bd9-ccc7-42b9-9874-12dd9c875579';

 id                                   | key     | value
--------------------------------------+---------+-------------------------------------------------
 d1230bd9-ccc7-42b9-9874-12dd9c875579 |   email | XeLpx33Y0E8U@eKY08kd30XVKkc07QH297SJw.SVbnnwqn6
 d1230bd9-ccc7-42b9-9874-12dd9c875579 |    name |                                           Karen
 d1230bd9-ccc7-42b9-9874-12dd9c875579 | website |                                      https://90
```

Corruption where primary keys are blank or from across tables has also been observed and is likely to appear in the cdc output from the yb cdc jar above.

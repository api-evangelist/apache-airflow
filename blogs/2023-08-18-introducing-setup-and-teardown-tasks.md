---
title: "Introducing Setup and Teardown tasks"
url: "https://airflow.apache.org/blog/introducing_setup_teardown/"
date: "2023-08-18T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
In data pipelines, commonly we need to create infrastructure resources, like a cluster or GPU nodes in an existing cluster, before doing the actual “work” and delete them after the work is done. Airflow 2.7 adds “setup” and “teardown” tasks to better support this type of pipeline. This blog post aims to highlight the key features so you know what’s possible.

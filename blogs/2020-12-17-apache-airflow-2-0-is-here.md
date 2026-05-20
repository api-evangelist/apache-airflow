---
title: "Apache Airflow 2.0 is here!"
url: "https://airflow.apache.org/blog/airflow-two-point-oh-is-here/"
date: "2020-12-17T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
I am proud to announce that Apache Airflow 2.0.0 has been released. The full changelog is about 3,000 lines long (already excluding everything backported to 1.10), so for now I’ll simply share some of the major features in 2.0.0 compared to 1.10.14: A new way of writing dags: the TaskFlow API (AIP-31) (Known in 2.0.0alphas as Functional DAGs.) DAGs are now much much nicer to author especially when using PythonOperator. Dependencies are handled more clearly and XCom is nicer to use Read more here: TaskFlow API Tutorial TaskFlow API Documentation A quick teaser of what DAGs can now look like:…

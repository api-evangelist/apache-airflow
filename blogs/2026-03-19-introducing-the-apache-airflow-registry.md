---
title: "Introducing the Apache Airflow Registry"
url: "https://airflow.apache.org/blog/airflow-registry/"
date: "2026-03-19T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>Today we&rsquo;re launching the <strong><a href="https://airflow.apache.org/registry/">Apache Airflow Registry</a></strong> — a searchable catalog of every official Airflow provider and its modules, live at <a href="https://airflow.apache.org/registry/">airflow.apache.org/registry/</a>.</p>
<p>Need an S3 operator? A Snowflake hook? An OpenAI sensor? The Registry helps you find, compare, and configure the right components for your data pipelines — without digging through docs or PyPI pages.</p>
<p><img alt="Registry Homepage" src="/blog/airflow-registry/images/registry-homepage.png" /></p>
<h2 id="by-the-numbers">By the Numbers</h2>
<table>
  <thead>
      <tr>
          <th></th>
          <th></th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><strong>98</strong></td>
          <td>Official providers</td>
      </tr>
      <tr>
          <td><strong>1,602</strong></td>
          <td>Modules (operators, hooks, sensors, triggers, transfers, and more)</td>
      </tr>
      <tr>
          <td><strong>329M+</strong></td>
          <td>Monthly PyPI downloads across all providers</td>
      </tr>
      <tr>
          <td><strong>125+</strong></td>
          <td>Integrations with cloud platforms, databases, ML tools, and messaging services</td>
      </tr>
  </tbody>
</table>
<h2 id="search-everything">Search Everything</h2>
<p>Hit <strong>Cmd+K</strong> from any page and start typing. Results show up instantly, grouped by Providers and Modules, with type badges so you can tell a hook from an operator at a glance.</p>
<p><img alt="Search results showing the S3Hook from the Amazon provider" src="/blog/airflow-registry/images/search.png" /></p>
<h2 id="provider-pages">Provider Pages</h2>
<p>Each provider gets a dedicated page with everything in one place: install command with copy-to-clipboard, version selector, extras dropdown, compatibility info, connection types, and the full module listing organized by type.</p>
<p><img alt="Amazon provider detail page showing 372 modules across 10 types" src="/blog/airflow-registry/images/provider-detail.png" /></p>
<p>The Amazon provider, for example, has <strong>372 modules</strong> across operators, hooks, sensors, triggers, transfers, and more. Module type tabs let you filter to exactly what you&rsquo;re looking for, and a category sidebar groups modules by AWS service (S3, Lambda, Glue, Step Functions, etc.).</p>
<h2 id="connection-builder">Connection Builder</h2>
<p>Click any connection type badge on a provider page, fill in the fields, and the builder generates the connection in three formats — <strong>URI</strong>, <strong>JSON</strong>, and <strong>Env Var</strong> — ready to copy into your configuration.</p>
<p><img alt="Connection builder showing URI, JSON, and Env Var export formats" src="/blog/airflow-registry/images/connection-builder.gif" /></p>
<p>No more guessing URI encoding or JSON structure.</p>
<h2 id="explore-by-category">Explore by Category</h2>
<p>Not sure which provider you need? The <strong><a href="https://airflow.apache.org/registry/explore/">Explore page</a></strong> organizes providers into categories: Cloud Platforms, Databases, Data Warehouses, Messaging &amp; Notifications, AI &amp; Machine Learning, Data Processing, and more.</p>
<p><img alt="Explore page showing providers grouped by category" src="/blog/airflow-registry/images/explore-categories.png" /></p>
<h2 id="statistics">Statistics</h2>
<p>The <strong><a href="https://airflow.apache.org/registry/stats/">Stats page</a></strong> breaks down the ecosystem: <strong>848 operators</strong>, <strong>298 hooks</strong>, <strong>164 triggers</strong>, <strong>157 sensors</strong>, <strong>83 transfers</strong>, and more — plus top providers by downloads and module count.</p>
<p><img alt="Registry statistics showing module distribution by type" src="/blog/airflow-registry/images/stats-page.png" /></p>
<h2 id="json-api">JSON API</h2>
<p>Every piece of data in the Registry is available as structured JSON — providers, modules, parameters, connections, versions. An <strong><a href="https://airflow.apache.org/registry/api-explorer/">API Explorer</a></strong> lets you browse all endpoints interactively.</p>
<p><img alt="API Explorer with OpenAPI 3.1 spec" src="/blog/airflow-registry/images/api-explorer.png" /></p>
<p>This makes the Registry accessible to IDE extensions, AI coding assistants, and automation tools.</p>
<h2 id="light--dark-mode">Light &amp; Dark Mode</h2>
<p>Full theme support with dark mode as the default. One click to switch.</p>
<p><img alt="Registry homepage in light mode" src="/blog/airflow-registry/images/light-mode.png" /></p>
<h2 id="standing-on-shoulders">Standing on Shoulders</h2>
<p>The Apache Airflow PMC would like to thank <a href="https://www.astronomer.io">Astronomer</a> for building and maintaining the Astronomer Registry for years — it was the go-to place to discover Airflow providers and proved the value of a searchable provider catalog. That work directly shaped this community-owned registry.</p>
<p>The Apache Airflow Registry lives at <code>airflow.apache.org</code>, is built from the same repo as the providers, and updates automatically when new versions are published.</p>
<h2 id="whats-next">What&rsquo;s Next</h2>
<p>This is the first release of the Registry. Here&rsquo;s what&rsquo;s coming:</p>
<ul>
<li><strong>Third-party provider support</strong> — we&rsquo;re exploring options to list community-built providers alongside the official ones</li>
<li><strong>Richer module pages</strong> — dedicated pages per module with full parameter docs and usage examples</li>
</ul>
<h2 id="get-involved">Get Involved</h2>
<ul>
<li><strong><a href="https://airflow.apache.org/registry/">Explore the Registry</a></strong> and let us know what you think</li>
<li><strong>Join the conversation</strong> on <a href="https://s.apache.org/airflow-slack">Airflow Slack</a> and the <a href="https://airflow.apache.org/community/">dev mailing list</a></li>
<li><strong>Contribute</strong> — the code lives in <a href="https://github.com/apache/airflow/tree/main/registry"><code>registry/</code></a> in the main Airflow repo</li>
<li><strong>Report issues or request features</strong> on <a href="https://github.com/apache/airflow/issues">GitHub</a></li>
</ul>

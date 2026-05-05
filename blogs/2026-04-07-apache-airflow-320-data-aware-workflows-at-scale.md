---
title: "Apache Airflow 3.2.0: Data-Aware Workflows at Scale"
url: "https://airflow.apache.org/blog/airflow-3.2.0/"
date: "2026-04-07T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>We&rsquo;re proud to announce the release of <strong>Apache Airflow 3.2.0</strong>! Airflow 3.1 puts humans at the center of automated workflows. 3.2 brings that same precision to data: Asset partitioning for granular pipeline orchestration, multi-team deployments for enterprise scale, synchronous deadline alert callbacks, and continued progress toward full Task SDK separation.</p>
<p><strong>Details</strong>:</p>
<p>📦 PyPI: <a href="https://pypi.org/project/apache-airflow/3.2.0/">https://pypi.org/project/apache-airflow/3.2.0/</a> <br />
📚 Docs: <a href="https://airflow.apache.org/docs/apache-airflow/3.2.0/">https://airflow.apache.org/docs/apache-airflow/3.2.0/</a> <br />
🛠️ Release Notes: <a href="https://airflow.apache.org/docs/apache-airflow/3.2.0/release_notes.html">https://airflow.apache.org/docs/apache-airflow/3.2.0/release_notes.html</a> <br />
🐳 Docker Image: <code>docker pull apache/airflow:3.2.0</code> <br />
🚏 Constraints: <a href="https://github.com/apache/airflow/tree/constraints-3.2.0">https://github.com/apache/airflow/tree/constraints-3.2.0</a></p>
<h1 id="-asset-partitioning-aip-76-only-the-right-work-gets-triggered">🗂️ Asset Partitioning (AIP-76): Only the Right Work Gets Triggered</h1>
<p>Asset partitioning has been one of the most requested additions to data-aware scheduling. If you work with date-partitioned S3 paths, Hive table partitions, BigQuery partitions, or really any partitioned data store, you&rsquo;ve dealt with this: An upstream task updates one partition, and every downstream Dag fires regardless of which slice actually changed. It&rsquo;s wasteful, and for large deployments it creates real operational noise.</p>
<p>Asset partitioning in 3.2 makes this granular. Downstream Dags trigger only when the specific partition they care about gets updated. It&rsquo;s the biggest change to data-aware scheduling since Assets were introduced, and it turns partition-driven orchestration into something Airflow handles natively rather than something you work around.</p>
<p><img alt="Asset Partitioning" src="/blog/airflow-3.2.0/images/asset_partitioning.png" /></p>
<h2 id="key-capabilities">Key Capabilities</h2>
<ul>
<li><strong>Partition-driven scheduling</strong>: Dags trigger on specific partition updates, not every asset change</li>
<li><strong>CronPartitionTimetable</strong>: Schedule Dags against partitions using cron expressions. Also available in the Task SDK</li>
<li><strong>Backfill for partitioned Dags</strong>: Backfill historical partitions without re-triggering everything downstream (#61464)</li>
<li><strong>Multi-asset partitions</strong>: A single Dag can listen for partitions across multiple assets, which matters when your downstream work depends on several sources aligning (#60577)</li>
</ul>
<p>For more advanced use cases, there are temporal and range partition mappers (#61522, #55247) for mapping time ranges and value ranges to partition keys, a partition key field on Dag run references (#61725) so you can inspect exactly which partition triggered a run, and PartitionedAssetTimetable for full control over how partition events from multiple assets get resolved into a unified trigger.</p>
<p><strong>Example</strong>: Three upstream ingestion Dags each write to a separate asset on an hourly cadence. The downstream Dag only triggers when all three have updated the same hourly partition. Since the three assets don&rsquo;t share a partition key natively, a mapper resolves them into a common key.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-py"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">__future__</span> <span class="kn">import</span> <span class="n">annotations</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.sdk</span> <span class="kn">import</span> <span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">DAG</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">Asset</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">CronPartitionTimetable</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">PartitionedAssetTimetable</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">StartOfHourMapper</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">asset</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">task</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">team_a_player_stats</span> <span class="o">=</span> <span class="n">Asset</span><span class="p">(</span><span class="n">uri</span><span class="o">=</span><span class="s2">"file://incoming/player-stats/team_a.csv"</span><span class="p">,</span> <span class="n">name</span><span class="o">=</span><span class="s2">"team_a_player_stats"</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="n">combined_player_stats</span> <span class="o">=</span> <span class="n">Asset</span><span class="p">(</span><span class="n">uri</span><span class="o">=</span><span class="s2">"file://curated/player-stats/combined.csv"</span><span class="p">,</span> <span class="n">name</span><span class="o">=</span><span class="s2">"combined_player_stats"</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">with</span> <span class="n">DAG</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">dag_id</span><span class="o">=</span><span class="s2">"ingest_team_a_player_stats"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">schedule</span><span class="o">=</span><span class="n">CronPartitionTimetable</span><span class="p">(</span><span class="s2">"0 * * * *"</span><span class="p">,</span> <span class="n">timezone</span><span class="o">=</span><span class="s2">"UTC"</span><span class="p">),</span>
</span></span><span class="line"><span class="cl">    <span class="n">tags</span><span class="o">=</span><span class="p">[</span><span class="s2">"player-stats"</span><span class="p">,</span> <span class="s2">"ingestion"</span><span class="p">],</span>
</span></span><span class="line"><span class="cl"><span class="p">):</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nd">@task</span><span class="p">(</span><span class="n">outlets</span><span class="o">=</span><span class="p">[</span><span class="n">team_a_player_stats</span><span class="p">])</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">ingest_team_a_stats</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">        <span class="s2">"""Materialize Team A player statistics for the current hourly partition."""</span>
</span></span><span class="line"><span class="cl">        <span class="k">pass</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">ingest_team_a_stats</span><span class="p">()</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="nd">@asset</span><span class="p">(</span><span class="n">schedule</span><span class="o">=</span><span class="n">CronPartitionTimetable</span><span class="p">(</span><span class="s2">"15 * * * *"</span><span class="p">,</span> <span class="n">timezone</span><span class="o">=</span><span class="s2">"UTC"</span><span class="p">))</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">team_b_player_stats</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">    <span class="k">pass</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">with</span> <span class="n">DAG</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">dag_id</span><span class="o">=</span><span class="s2">"clean_and_combine_player_stats"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">schedule</span><span class="o">=</span><span class="n">PartitionedAssetTimetable</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">assets</span><span class="o">=</span><span class="n">team_a_player_stats</span> <span class="o">&amp;</span> <span class="n">team_b_player_stats</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">default_partition_mapper</span><span class="o">=</span><span class="n">StartOfHourMapper</span><span class="p">(),</span>
</span></span><span class="line"><span class="cl">    <span class="p">),</span>
</span></span><span class="line"><span class="cl">    <span class="n">catchup</span><span class="o">=</span><span class="kc">False</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">):</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nd">@task</span><span class="p">(</span><span class="n">outlets</span><span class="o">=</span><span class="p">[</span><span class="n">combined_player_stats</span><span class="p">])</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">combine_player_stats</span><span class="p">(</span><span class="n">dag_run</span><span class="o">=</span><span class="kc">None</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">        <span class="s2">"""Merge the aligned hourly partitions into a combined dataset."""</span>
</span></span><span class="line"><span class="cl">        <span class="nb">print</span><span class="p">(</span><span class="n">dag_run</span><span class="o">.</span><span class="n">partition_key</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">combine_player_stats</span><span class="p">()</span>
</span></span></code></pre></div><p>See <a href="https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/example_dags/example_asset_partition.py"><code>example_asset_partition.py</code></a> and the Task SDK API docs for <code>PartitionedAssetTimetable</code> and partition mappers.</p>
<h1 id="-multi-team-deployments-aip-67-airflow-for-the-enterprise">🏢 Multi-Team Deployments (AIP-67): Airflow for the Enterprise</h1>
<blockquote>
<p>⚠️ <strong>Experimental</strong>: Multi-Team support is experimental in Airflow 3.2 and may change in future releases based on user feedback.</p></blockquote>
<p>Airflow 3.2 introduces multi-team support, allowing organizations to run multiple isolated teams within a single Airflow deployment. Each team can have its own Dags, connections, variables, pools, and executors— enabling true resource and permission isolation without requiring separate Airflow instances per team.</p>
<p>This is particularly valuable for platform teams that serve multiple data engineering or data science teams from shared infrastructure, while maintaining strong boundaries between teams&rsquo; resources and access.</p>
<h2 id="key-capabilities-1">Key Capabilities</h2>
<ul>
<li><strong>Per-team resource isolation</strong>: Each team has its own Dags, connections, variables, and pools</li>
<li><strong>Per-team executors</strong>: Different teams can use different executors (e.g. Celery, Kubernetes, Local, AWS ECS, etc.) and configure them separately — #57837, #57910</li>
<li><strong>Team-scoped authorization</strong>: Keycloak and Simple auth managers support team-scoped access control (#61351, #61861)</li>
<li><strong>Team-scoped secrets</strong>: Use <code>AIRFLOW_VAR__{TEAM}___{KEY}</code> environment variable or <code>AIRFLOW_CONN__&lt;TEAM&gt;___&lt;CONN_ID&gt;</code> pattern for team-specific secrets (#62588)</li>
<li><strong>CLI management</strong>: New CLI commands for managing teams (#55283)</li>
<li><strong>UI team selector</strong>: Team selector in connection, variable, and pool create/edit forms (#60237, #60474, #61082)</li>
<li><strong>Full API support</strong>: <code>team_name</code> field added to Connection, Variable, and Pool APIs (#59336, #57102, #60952)</li>
</ul>
<h2 id="enabling-multi-team">Enabling Multi-Team</h2>
<pre tabindex="0"><code># In airflow.cfg:
[core]
multi_team = True

# Or via environment variable:
export AIRFLOW__CORE__MULTI_TEAM=True
</code></pre><h1 id="-deadline-alerts-now-with-synchronous-callbacks-aip-86">⏰ Deadline Alerts: Now With Synchronous Callbacks (AIP-86)</h1>
<blockquote>
<p>⚠️ <strong>Experimental</strong>: Deadline Alerts are experimental in Airflow 3.2 and may change in future releases based on user feedback.</p></blockquote>
<p>Building on the Deadline Alerts system introduced in Airflow 3.1, this release adds synchronous callback support. In 3.1, callbacks ran through the triggerer (async only), which limited integration options. Synchronous callbacks execute directly via the executor, with optional targeting of a specific executor via the executor parameter.</p>
<h2 id="whats-new-in-32">What&rsquo;s New in 3.2</h2>
<ul>
<li><strong>SyncCallback support</strong>: Unlike <code>AsyncCallback</code> which runs on the triggerer, <code>SyncCallback</code> executes directly on the worker via the executor, with optional targeting of a specific executor</li>
<li><strong>Multiple Deadline Alerts per Dag</strong>: Pass a list to the deadline parameter to configure multiple thresholds on a single Dag</li>
<li><strong>Missed-deadline metadata in Grid API</strong>: Dag run API now includes missed-deadline information for programmatic monitoring</li>
<li><strong>Improved UX for custom DeadlineReferences</strong>: Cleaner developer experience when defining custom deadline reference points (#57222)</li>
</ul>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-py"><span class="line"><span class="cl"><span class="k">with</span> <span class="n">DAG</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">dag_id</span><span class="o">=</span><span class="s2">"sync_deadline"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">deadline</span><span class="o">=</span><span class="n">DeadlineAlert</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">reference</span><span class="o">=</span><span class="n">DeadlineReference</span><span class="o">.</span><span class="n">FIXED_DATETIME</span><span class="p">(</span><span class="n">datetime</span><span class="p">(</span><span class="mi">1980</span><span class="p">,</span> <span class="mi">8</span><span class="p">,</span> <span class="mi">10</span><span class="p">,</span> <span class="mi">2</span><span class="p">)),</span>
</span></span><span class="line"><span class="cl">        <span class="n">interval</span><span class="o">=</span><span class="n">timedelta</span><span class="p">(</span><span class="mi">0</span><span class="p">),</span>
</span></span><span class="line"><span class="cl">        <span class="n">callback</span><span class="o">=</span><span class="n">SyncCallback</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">            <span class="n">SlackWebhookNotifier</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">            <span class="p">{</span><span class="s2">"text"</span><span class="p">:</span> <span class="s2">"Sync Callback; Alert should trigger immediately!"</span><span class="p">},</span>
</span></span><span class="line"><span class="cl">        <span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="p">):</span>
</span></span><span class="line"><span class="cl">    <span class="n">EmptyOperator</span><span class="p">(</span><span class="n">task_id</span><span class="o">=</span><span class="s1">'empty_task'</span><span class="p">)</span>
</span></span></code></pre></div><h1 id="-ui-enhancements">🖥️ UI Enhancements</h1>
<ul>
<li><strong>HITL Approval History</strong>: The Human-in-the-Loop approval interface now shows the complete audit trail of approvals and rejections for any task. (#56760, #55952)</li>
<li><strong>XCom Management</strong>: You can now add, edit, and delete XCom values directly from the UI. (#58921)</li>
<li><strong>Segmented state bar</strong>: Collapsed task groups and mapped tasks now show a segmented state bar for at-a-glance status (#61854)</li>
<li><strong>Unified tooltips</strong>: Grid and Graph view tooltips now show dates, duration, and child states (#62119)</li>
<li><strong>Filename in Dag Code tab</strong>: File identification now shown in the Code tab (#60759)</li>
<li><strong>Copy button for logs</strong>: One-click log copying (#61185)</li>
<li><strong>Date range filter</strong>: Filter Dag executions by date range (#60772)</li>
<li><strong>Task upstream/downstream filter</strong>: Filter by upstream or downstream tasks in Graph and Grid views (#57237)</li>
<li><strong>Data redaction</strong>: Sensitive fields are now redacted in the UI and Public API (#59873)</li>
<li><strong>Custom theme support</strong>: <code>globalCss</code> and theme config for white-label/custom deployments (#61161, #58411)</li>
<li><strong>Inherit core UI theme in React plugins</strong>: Plugin UIs now automatically match the core Airflow theme (#60256)</li>
<li><strong>Task display names in Gantt</strong>: <code>task_display_name</code> shown for better readability (#61438)</li>
</ul>
<h1 id="-performance-improvements">🚀 Performance Improvements</h1>
<p><strong>Rendered Task Instance Fields Cleanup: ~42x Faster.</strong> The cleanup job for rendered task instance fields has been rewritten and is roughly 42 times faster for Dags with many mapped tasks. Retention is now based on the N most recent Dag runs rather than N most recent task executions, which is both more intuitive and dramatically more performant. Config renamed: <code>max_num_rendered_ti_fields_per_task</code> → <code>num_dag_runs_to_retain_rendered_fields</code> (old name still works with deprecation warning). (#60951)</p>
<p><strong>Scheduler Improvements.</strong> For large-scale deployments, 3.2 addresses several known bottlenecks:</p>
<ul>
<li>The scheduler no longer loads all TaskInstances into memory, preventing memory spikes on large deployments (#60956)</li>
<li>Faster task dequeuing loop (#61376)</li>
<li>Queue query now enforces <code>max_active_tasks</code> directly, preventing over-queueing (#54103)</li>
</ul>
<p><strong>API Server Improvements:</strong></p>
<ul>
<li>Eliminated SerializedDag loads on task start, reducing memory usage (#60803)</li>
<li><code>serialized_dag</code> data column now uses JSONB on PostgreSQL (#55979)</li>
</ul>
<h1 id="-task-sdk-evolution--developer-experience">🔧 Task SDK Evolution &amp; Developer Experience</h1>
<h2 id="task-sdk-decoupling-continues">Task SDK Decoupling Continues</h2>
<p>Airflow 3.2 continues moving components from <code>airflow-core</code> into the Task SDK, progressing toward full client-server separation. This enables Dag authors to independently upgrade the Task SDK without requiring Airflow Core upgrades, reducing coordination overhead between Dag authors and Ops teams.</p>
<p>Modules moved to Task SDK in this release (old import paths still work with deprecation warnings):</p>
<ul>
<li><strong>Exceptions</strong>: <code>AirflowSkipException</code>, <code>TaskDeferred</code>, etc. → <code>airflow.sdk.exceptions</code> (#59780)</li>
<li><strong>Serde</strong>: <code>airflow.serialization.serde</code> → <code>airflow.sdk.serde</code>; serializers → <code>airflow.sdk.serde.serializers.*</code> (#58900)</li>
<li><strong>SkipMixin / BranchMixIn</strong>: Moved to Task SDK; existing imports work via <code>common-compat</code> (#62749, #62776)</li>
<li><strong>Lineage module</strong>: Moved to Task SDK for client-server separation (#60968, #61157)</li>
<li><strong>Listeners module</strong>: Moved to shared library (#59883)</li>
<li><strong>XCom API</strong>: Decoupled from <code>XComEncoder</code> (#58900)</li>
</ul>
<h2 id="pythonoperator-async-support">PythonOperator Async Support</h2>
<p><code>PythonOperator</code> now supports async callables. You can pass an async function as the <code>python_callable</code> and the operator will correctly await it, enabling async I/O patterns without needing a custom operator. (#60268)</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-py"><span class="line"><span class="cl"><span class="nd">@task</span><span class="p">(</span><span class="n">show_return_value_in_logs</span><span class="o">=</span><span class="kc">False</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">async</span> <span class="k">def</span> <span class="nf">load_xml_files</span><span class="p">(</span><span class="n">files</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">    <span class="kn">import</span> <span class="nn">asyncio</span>
</span></span><span class="line"><span class="cl">    <span class="kn">from</span> <span class="nn">io</span> <span class="kn">import</span> <span class="n">BytesIO</span>
</span></span><span class="line"><span class="cl">    <span class="kn">from</span> <span class="nn">more_itertools</span> <span class="kn">import</span> <span class="n">chunked</span>
</span></span><span class="line"><span class="cl">    <span class="kn">from</span> <span class="nn">os</span> <span class="kn">import</span> <span class="n">cpu_count</span>
</span></span><span class="line"><span class="cl">    <span class="kn">from</span> <span class="nn">tenacity</span> <span class="kn">import</span> <span class="n">retry</span><span class="p">,</span> <span class="n">stop_after_attempt</span><span class="p">,</span> <span class="n">wait_fixed</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="kn">from</span> <span class="nn">airflow.providers.sftp.hooks.sftp</span> <span class="kn">import</span> <span class="n">SFTPClientPool</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nb">print</span><span class="p">(</span><span class="s2">"number of files:"</span><span class="p">,</span> <span class="nb">len</span><span class="p">(</span><span class="n">files</span><span class="p">))</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="k">async</span> <span class="k">with</span> <span class="n">SFTPClientPool</span><span class="p">(</span><span class="n">sftp_conn_id</span><span class="o">=</span><span class="n">sftp_conn</span><span class="p">,</span> <span class="n">pool_size</span><span class="o">=</span><span class="n">cpu_count</span><span class="p">())</span> <span class="k">as</span> <span class="n">pool</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">        <span class="nd">@retry</span><span class="p">(</span><span class="n">stop</span><span class="o">=</span><span class="n">stop_after_attempt</span><span class="p">(</span><span class="mi">3</span><span class="p">),</span> <span class="n">wait</span><span class="o">=</span><span class="n">wait_fixed</span><span class="p">(</span><span class="mi">5</span><span class="p">))</span>
</span></span><span class="line"><span class="cl">        <span class="k">async</span> <span class="k">def</span> <span class="nf">download_file</span><span class="p">(</span><span class="n">file</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">            <span class="k">async</span> <span class="k">with</span> <span class="n">pool</span><span class="o">.</span><span class="n">get_sftp_client</span><span class="p">()</span> <span class="k">as</span> <span class="n">sftp</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">                <span class="nb">print</span><span class="p">(</span><span class="s2">"downloading:"</span><span class="p">,</span> <span class="n">file</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">                <span class="n">buffer</span> <span class="o">=</span> <span class="n">BytesIO</span><span class="p">()</span>
</span></span><span class="line"><span class="cl">                <span class="k">async</span> <span class="k">with</span> <span class="n">sftp</span><span class="o">.</span><span class="n">open</span><span class="p">(</span><span class="n">file</span><span class="p">,</span> <span class="n">encoding</span><span class="o">=</span><span class="n">xml_encoding</span><span class="p">)</span> <span class="k">as</span> <span class="n">remote_file</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">                    <span class="n">data</span> <span class="o">=</span> <span class="k">await</span> <span class="n">remote_file</span><span class="o">.</span><span class="n">read</span><span class="p">()</span>
</span></span><span class="line"><span class="cl">                    <span class="n">buffer</span><span class="o">.</span><span class="n">write</span><span class="p">(</span><span class="n">data</span><span class="o">.</span><span class="n">encode</span><span class="p">(</span><span class="n">xml_encoding</span><span class="p">))</span>
</span></span><span class="line"><span class="cl">                    <span class="n">buffer</span><span class="o">.</span><span class="n">seek</span><span class="p">(</span><span class="mi">0</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">                <span class="k">return</span> <span class="n">buffer</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">        <span class="k">for</span> <span class="n">batch</span> <span class="ow">in</span> <span class="n">chunked</span><span class="p">(</span><span class="n">files</span><span class="p">,</span> <span class="n">cpu_count</span><span class="p">()</span> <span class="o">*</span> <span class="mi">2</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">            <span class="n">tasks</span> <span class="o">=</span> <span class="p">[</span><span class="n">asyncio</span><span class="o">.</span><span class="n">create_task</span><span class="p">(</span><span class="n">download_file</span><span class="p">(</span><span class="n">f</span><span class="p">))</span> <span class="k">for</span> <span class="n">f</span> <span class="ow">in</span> <span class="n">batch</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">            <span class="c1"># Wait for this batch to finish before starting the next</span>
</span></span><span class="line"><span class="cl">            <span class="k">for</span> <span class="n">task</span> <span class="ow">in</span> <span class="n">asyncio</span><span class="o">.</span><span class="n">as_completed</span><span class="p">(</span><span class="n">tasks</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">                <span class="n">result</span> <span class="o">=</span> <span class="k">await</span> <span class="n">task</span>
</span></span><span class="line"><span class="cl">                <span class="c1"># Do something with result or accumulate it and return it as an XCom</span>
</span></span></code></pre></div><h1 id="updated-security-model">Updated security model</h1>
<p>We are working on improving isolation and improving security of Airflow deployments and in order to make our users better informed of what expectations they should have for Airflow security, we updated the security model to reflect changes implemented in Airflow 3.2.0 and explain future improvements that we work on in this area. See more:  <a href="https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html">Airflow Security Model</a>.</p>
<h1 id="-community-appreciation">🙏 Community Appreciation</h1>
<p>This release represents the collaborative effort of hundreds of contributors from around the world. Special thanks to our release manager and all the developers, documentarians, testers, and community members who made Airflow 3.2.0 possible.</p>
<p>Thanks to contributors like you, the Airflow project continues to thrive. Whether you&rsquo;re filing issues, submitting PRs, improving documentation, or helping others in the community, every contribution matters.</p>
<h1 id="-get-involved">🔗 Get Involved</h1>
<ul>
<li><strong>Try the Release</strong>: Upgrade your development environment and explore the new features</li>
<li><strong>Join the Conversation</strong>: Connect with us on <a href="https://s.apache.org/airflow-slack">Slack</a> and the <a href="https://airflow.apache.org/community/">dev mailing list</a></li>
<li><strong>Contribute</strong>: Check out our <a href="https://github.com/apache/airflow/blob/main/contributing-docs/README.rst">contribution guide</a></li>
<li><strong>Provide Feedback</strong>: Share your experiences and suggestions on <a href="https://github.com/apache/airflow">GitHub</a></li>
</ul>
<p>Apache Airflow 3.2.0 marks a new chapter in data-aware, partition-driven workflow orchestration. We can&rsquo;t wait to see what you build with it!</p>

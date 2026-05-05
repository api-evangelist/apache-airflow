---
title: "Ask Your Survey Anything: Building AI Analysis Pipelines with Airflow 3"
url: "https://airflow.apache.org/blog/ai-survey-analysis-pipelines/"
date: "2026-04-15T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>The <a href="https://airflow.apache.org/survey/">2025 Airflow Community Survey</a> collected responses
from nearly 6,000 practitioners across 168 questions. You can open a spreadsheet and filter,
or write SQL by hand. But what if you could just ask a question and have Airflow figure out
the query, run it, and bring the result back for your approval?</p>
<p>This post builds two pipelines that do exactly that, using the
<a href="https://pypi.org/project/apache-airflow-providers-common-ai/"><code>apache-airflow-providers-common-ai</code></a>
provider for Airflow 3.</p>
<p>The first pipeline is <strong>interactive</strong>: a human reviews the question before it reaches the LLM
and approves the result before the DAG finishes. The second is <strong>scheduled</strong>: it downloads
fresh survey data, validates the schema, runs the query unattended, and emails the result.</p>
<p>If you haven&rsquo;t seen the <a href="https://airflow.apache.org/blog/common-ai-provider/">common.ai provider overview</a> yet, start there for a tour of all the
operators. This post goes deep on a concrete end-to-end example.</p>
<h2 id="two-pipelines-one-example-file">Two Pipelines, One Example File</h2>
<p>Both DAGs live in
<a href="https://github.com/apache/airflow/tree/main/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_analysis.py"><code>example_llm_survey_analysis.py</code></a>
and share the same schema context and datasource configuration.</p>
<p><strong><code>example_llm_survey_interactive</code></strong>: trigger manually, review at both ends:</p>
<pre tabindex="0"><code>prompt_confirmation  →  generate_sql  →  run_query  →  extract_data  →  result_confirmation
(HITLEntryOperator)     (LLMSQLQuery)    (Analytics)    (@task)          (ApprovalOperator)
</code></pre><p><strong><code>example_llm_survey_scheduled</code></strong>: runs <code>@monthly</code>, no human in the loop:</p>
<pre tabindex="0"><code>download_survey  →  prepare_csv  →  check_schema  →  generate_sql  →  run_query  →  extract_data  →  send_result
(HttpOperator)      (@task)         (LLMSchema       (LLMSQLQuery)    (Analytics)    (@task)          (@task / Email)
                                     Compare)
</code></pre><h2 id="the-data">The Data</h2>
<p>The <a href="https://airflow.apache.org/survey/">Airflow Community Survey 2025</a> CSV has 5,856 rows
and 168 columns covering everything from Airflow version and executor type to cloud provider,
company size, and AI tool usage. A few highlights from the data:</p>
<ul>
<li><strong>3,320</strong> respondents identify as Data Engineers</li>
<li><strong>2,032</strong> use AWS as their primary cloud provider for Airflow</li>
<li><strong>1,445</strong> are already running Airflow 3</li>
<li><strong>1,351</strong> say they <em>often</em> use AI tools to write Airflow code</li>
</ul>
<p>Those last two numbers together are part of why this example exists: the people most likely
to use this pipeline are already using Airflow 3 and already using AI in their workflow.</p>
<blockquote>
<p><strong>Data prep note:</strong> Apache DataFusion is strict about CSV schemas. The raw survey export
has 22 duplicate <code>&quot;Other&quot;</code> column names and some free-text cells with embedded newlines.
Both need cleaning before DataFusion will parse the file. The interactive DAG assumes a
cleaned copy at the path set by the <code>SURVEY_CSV_PATH</code> environment variable. The scheduled
DAG downloads the file at runtime and the <code>prepare_csv</code> step handles writing it to disk.</p></blockquote>
<h2 id="the-interactive-pipeline">The Interactive Pipeline</h2>
<p>Five tasks. No external services beyond your LLM provider and a local copy of the CSV.</p>
<table>
  <thead>
      <tr>
          <th>Step</th>
          <th>Operator</th>
          <th>What happens</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>1</td>
          <td><code>HITLEntryOperator</code></td>
          <td>DAG pauses. Human reviews and optionally edits the question.</td>
      </tr>
      <tr>
          <td>2</td>
          <td><code>LLMSQLQueryOperator</code></td>
          <td>LLM translates the confirmed question into SQL, validated by sqlglot.</td>
      </tr>
      <tr>
          <td>3</td>
          <td><code>AnalyticsOperator</code></td>
          <td>Apache DataFusion executes the SQL against the local CSV.</td>
      </tr>
      <tr>
          <td>4</td>
          <td><code>@task extract_data</code></td>
          <td>Strips the query from the JSON result so the reviewer sees only data rows.</td>
      </tr>
      <tr>
          <td>5</td>
          <td><code>ApprovalOperator</code></td>
          <td>DAG pauses again. Human approves or rejects the result.</td>
      </tr>
  </tbody>
</table>
<p>The LLM and DataFusion steps run unattended. The human shows up at step 1 to confirm the
question and at step 5 to sign off on the answer. Everything in between is automated.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="nd">@dag</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">example_llm_survey_interactive</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">prompt_confirmation</span> <span class="o">=</span> <span class="n">HITLEntryOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"prompt_confirmation"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">subject</span><span class="o">=</span><span class="s2">"Review the survey analysis question"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">params</span><span class="o">=</span><span class="p">{</span>
</span></span><span class="line"><span class="cl">            <span class="s2">"prompt"</span><span class="p">:</span> <span class="n">Param</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">                <span class="s2">"How does AI tool usage for writing Airflow code compare "</span>
</span></span><span class="line"><span class="cl">                <span class="s2">"between Airflow 3 users and Airflow 2 users?"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">                <span class="nb">type</span><span class="o">=</span><span class="s2">"string"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">                <span class="n">description</span><span class="o">=</span><span class="s2">"The natural language question to answer via SQL"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">            <span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="p">},</span>
</span></span><span class="line"><span class="cl">        <span class="n">response_timeout</span><span class="o">=</span><span class="n">datetime</span><span class="o">.</span><span class="n">timedelta</span><span class="p">(</span><span class="n">hours</span><span class="o">=</span><span class="mi">1</span><span class="p">),</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">generate_sql</span> <span class="o">=</span> <span class="n">LLMSQLQueryOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"generate_sql"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">prompt</span><span class="o">=</span><span class="s2">"{{ ti.xcom_pull(task_ids='prompt_confirmation')['params_input']['prompt'] }}"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">llm_conn_id</span><span class="o">=</span><span class="n">LLM_CONN_ID</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">datasource_config</span><span class="o">=</span><span class="n">survey_datasource</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">schema_context</span><span class="o">=</span><span class="n">SURVEY_SCHEMA</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">run_query</span> <span class="o">=</span> <span class="n">AnalyticsOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"run_query"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">datasource_configs</span><span class="o">=</span><span class="p">[</span><span class="n">survey_datasource</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">        <span class="n">queries</span><span class="o">=</span><span class="p">[</span><span class="s2">"{{ ti.xcom_pull(task_ids='generate_sql') }}"</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">        <span class="n">result_output_format</span><span class="o">=</span><span class="s2">"json"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nd">@task</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">extract_data</span><span class="p">(</span><span class="n">raw</span><span class="p">:</span> <span class="nb">str</span><span class="p">)</span> <span class="o">-&gt;</span> <span class="nb">str</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">        <span class="n">results</span> <span class="o">=</span> <span class="n">json</span><span class="o">.</span><span class="n">loads</span><span class="p">(</span><span class="n">raw</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="n">data</span> <span class="o">=</span> <span class="p">[</span><span class="n">row</span> <span class="k">for</span> <span class="n">item</span> <span class="ow">in</span> <span class="n">results</span> <span class="k">for</span> <span class="n">row</span> <span class="ow">in</span> <span class="n">item</span><span class="p">[</span><span class="s2">"data"</span><span class="p">]]</span>
</span></span><span class="line"><span class="cl">        <span class="k">return</span> <span class="n">json</span><span class="o">.</span><span class="n">dumps</span><span class="p">(</span><span class="n">data</span><span class="p">,</span> <span class="n">indent</span><span class="o">=</span><span class="mi">2</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">result_data</span> <span class="o">=</span> <span class="n">extract_data</span><span class="p">(</span><span class="n">run_query</span><span class="o">.</span><span class="n">output</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">result_confirmation</span> <span class="o">=</span> <span class="n">ApprovalOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"result_confirmation"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">subject</span><span class="o">=</span><span class="s2">"Review the survey query result"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">body</span><span class="o">=</span><span class="n">result_data</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">response_timeout</span><span class="o">=</span><span class="n">datetime</span><span class="o">.</span><span class="n">timedelta</span><span class="p">(</span><span class="n">hours</span><span class="o">=</span><span class="mi">1</span><span class="p">),</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">prompt_confirmation</span> <span class="o">&gt;&gt;</span> <span class="n">generate_sql</span> <span class="o">&gt;&gt;</span> <span class="n">run_query</span>
</span></span></code></pre></div><h2 id="walking-through-a-run">Walking Through a Run</h2>
<p><strong>Step 1: Prompt confirmation.</strong> Trigger the DAG and navigate to the HITL review tab.
The default question appears in an editable field. Change it to anything the schema supports,
or leave it as-is and confirm.</p>
<blockquote>
<p><em>&ldquo;How does AI tool usage for writing Airflow code compare between Airflow 3 users and Airflow 2 users?&rdquo;</em></p></blockquote>
<p><strong>Step 2: SQL generation.</strong> <code>LLMSQLQueryOperator</code> receives the confirmed question, constructs
a system prompt from <code>SURVEY_SCHEMA</code>, and calls the LLM. It returns validated SQL. sqlglot
parses the output and rejects anything that isn&rsquo;t a <code>SELECT</code>. The generated query goes to XCom.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-sql"><span class="line"><span class="cl"><span class="k">SELECT</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="s2">"Which version of Airflow do you currently use?"</span><span class="w">                                </span><span class="k">AS</span><span class="w"> </span><span class="n">airflow_version</span><span class="p">,</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="s2">"Are you using AI/LLM (ChatGPT/Cursor/Claude etc) to assist you in writing Airflow code?"</span><span class="w"> </span><span class="k">AS</span><span class="w"> </span><span class="n">ai_usage</span><span class="p">,</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="k">COUNT</span><span class="p">(</span><span class="o">*</span><span class="p">)</span><span class="w"> </span><span class="k">AS</span><span class="w"> </span><span class="n">respondents</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">FROM</span><span class="w"> </span><span class="n">survey</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">WHERE</span><span class="w"> </span><span class="s2">"Which version of Airflow do you currently use?"</span><span class="w"> </span><span class="k">IS</span><span class="w"> </span><span class="k">NOT</span><span class="w"> </span><span class="k">NULL</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">  </span><span class="k">AND</span><span class="w"> </span><span class="s2">"Are you using AI/LLM (ChatGPT/Cursor/Claude etc) to assist you in writing Airflow code?"</span><span class="w"> </span><span class="k">IS</span><span class="w"> </span><span class="k">NOT</span><span class="w"> </span><span class="k">NULL</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">GROUP</span><span class="w"> </span><span class="k">BY</span><span class="w"> </span><span class="n">airflow_version</span><span class="p">,</span><span class="w"> </span><span class="n">ai_usage</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">ORDER</span><span class="w"> </span><span class="k">BY</span><span class="w"> </span><span class="n">airflow_version</span><span class="p">,</span><span class="w"> </span><span class="n">respondents</span><span class="w"> </span><span class="k">DESC</span><span class="w">
</span></span></span></code></pre></div><p><strong>Step 3: DataFusion execution.</strong> <code>AnalyticsOperator</code> loads the CSV into a DataFusion
<code>SessionContext</code>, registers it as the <code>survey</code> table, and executes the SQL in-process.
No database server, no network call. The 5,856-row CSV runs in under a second.</p>
<p><strong>Step 4: Extract data.</strong> The raw JSON from <code>AnalyticsOperator</code> includes the original
query string alongside the result rows. This <code>@task</code> strips the query so the reviewer
isn&rsquo;t looking at SQL when they should be looking at data.</p>
<p><strong>Step 5: Result confirmation.</strong> The data rows appear in the Airflow UI approval dialog.
The analyst reads the result, clicks Approve (or Reject if something looks off), and the
DAG completes.</p>
<h2 id="the-scheduled-pipeline">The Scheduled Pipeline</h2>
<p>The scheduled variant adds three capabilities the interactive version intentionally omits:
data acquisition, schema validation, and result delivery. It runs <code>@monthly</code> (configurable)
with no human steps.</p>
<table>
  <thead>
      <tr>
          <th>Step</th>
          <th>Operator</th>
          <th>What happens</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>1</td>
          <td><code>HttpOperator</code></td>
          <td>Downloads the survey CSV from <code>airflow.apache.org</code>.</td>
      </tr>
      <tr>
          <td>2</td>
          <td><code>@task prepare_csv</code></td>
          <td>Writes the CSV to disk and generates a reference schema file from <code>SURVEY_SCHEMA</code>.</td>
      </tr>
      <tr>
          <td>3</td>
          <td><code>LLMSchemaCompareOperator</code></td>
          <td>LLM compares the downloaded CSV schema against the reference. Raises if critical columns are missing or renamed.</td>
      </tr>
      <tr>
          <td>4</td>
          <td><code>LLMSQLQueryOperator</code></td>
          <td>Translates the fixed question into validated SQL.</td>
      </tr>
      <tr>
          <td>5</td>
          <td><code>AnalyticsOperator</code></td>
          <td>Executes the SQL via DataFusion.</td>
      </tr>
      <tr>
          <td>6</td>
          <td><code>@task extract_data</code></td>
          <td>Extracts data rows from the JSON result.</td>
      </tr>
      <tr>
          <td>7</td>
          <td><code>@task send_result</code></td>
          <td>Sends the result via <code>SmtpHook</code> if <code>SMTP_CONN_ID</code> and <code>NOTIFY_EMAIL</code> are set, otherwise logs to the task log.</td>
      </tr>
  </tbody>
</table>
<p>The schema check at step 3 is worth calling out. <code>LLMSchemaCompareOperator</code> compares the
live download against a reference file derived from <code>SURVEY_SCHEMA</code>. If the survey format
changes between runs (a renamed column, a dropped field), the operator catches it before
any SQL runs, rather than failing silently mid-pipeline with a cryptic DataFusion error.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="nd">@dag</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">example_llm_survey_scheduled</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">download_survey</span> <span class="o">=</span> <span class="n">HttpOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"download_survey"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">http_conn_id</span><span class="o">=</span><span class="n">AIRFLOW_WEBSITE_CONN_ID</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">endpoint</span><span class="o">=</span><span class="n">SURVEY_CSV_ENDPOINT</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">method</span><span class="o">=</span><span class="s2">"GET"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">response_filter</span><span class="o">=</span><span class="k">lambda</span> <span class="n">r</span><span class="p">:</span> <span class="n">r</span><span class="o">.</span><span class="n">text</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">log_response</span><span class="o">=</span><span class="kc">False</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nd">@task</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">prepare_csv</span><span class="p">(</span><span class="n">csv_text</span><span class="p">:</span> <span class="nb">str</span><span class="p">)</span> <span class="o">-&gt;</span> <span class="kc">None</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">        <span class="n">os</span><span class="o">.</span><span class="n">makedirs</span><span class="p">(</span><span class="n">os</span><span class="o">.</span><span class="n">path</span><span class="o">.</span><span class="n">dirname</span><span class="p">(</span><span class="n">SURVEY_CSV_PATH</span><span class="p">),</span> <span class="n">exist_ok</span><span class="o">=</span><span class="kc">True</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">with</span> <span class="nb">open</span><span class="p">(</span><span class="n">SURVEY_CSV_PATH</span><span class="p">,</span> <span class="s2">"w"</span><span class="p">,</span> <span class="n">encoding</span><span class="o">=</span><span class="s2">"utf-8"</span><span class="p">)</span> <span class="k">as</span> <span class="n">f</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">            <span class="n">f</span><span class="o">.</span><span class="n">write</span><span class="p">(</span><span class="n">csv_text</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="n">columns</span> <span class="o">=</span> <span class="p">[</span><span class="n">line</span><span class="o">.</span><span class="n">split</span><span class="p">(</span><span class="s1">'"'</span><span class="p">)[</span><span class="mi">1</span><span class="p">]</span> <span class="k">for</span> <span class="n">line</span> <span class="ow">in</span> <span class="n">SURVEY_SCHEMA</span><span class="o">.</span><span class="n">strip</span><span class="p">()</span><span class="o">.</span><span class="n">splitlines</span><span class="p">()</span> <span class="k">if</span> <span class="s1">'"'</span> <span class="ow">in</span> <span class="n">line</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">        <span class="k">with</span> <span class="nb">open</span><span class="p">(</span><span class="n">REFERENCE_CSV_PATH</span><span class="p">,</span> <span class="s2">"w"</span><span class="p">,</span> <span class="n">newline</span><span class="o">=</span><span class="s2">""</span><span class="p">,</span> <span class="n">encoding</span><span class="o">=</span><span class="s2">"utf-8"</span><span class="p">)</span> <span class="k">as</span> <span class="n">ref</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">            <span class="n">csv_mod</span><span class="o">.</span><span class="n">writer</span><span class="p">(</span><span class="n">ref</span><span class="p">)</span><span class="o">.</span><span class="n">writerow</span><span class="p">(</span><span class="n">columns</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">csv_ready</span> <span class="o">=</span> <span class="n">prepare_csv</span><span class="p">(</span><span class="n">download_survey</span><span class="o">.</span><span class="n">output</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">check_schema</span> <span class="o">=</span> <span class="n">LLMSchemaCompareOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"check_schema"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">prompt</span><span class="o">=</span><span class="s2">"Compare the survey CSV schema against the reference. Flag missing or renamed columns."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">llm_conn_id</span><span class="o">=</span><span class="n">LLM_CONN_ID</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">data_sources</span><span class="o">=</span><span class="p">[</span><span class="n">survey_datasource</span><span class="p">,</span> <span class="n">reference_datasource</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">        <span class="n">context_strategy</span><span class="o">=</span><span class="s2">"basic"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="n">csv_ready</span> <span class="o">&gt;&gt;</span> <span class="n">check_schema</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">generate_sql</span> <span class="o">=</span> <span class="n">LLMSQLQueryOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"generate_sql"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">prompt</span><span class="o">=</span><span class="n">SCHEDULED_PROMPT</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">llm_conn_id</span><span class="o">=</span><span class="n">LLM_CONN_ID</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">datasource_config</span><span class="o">=</span><span class="n">survey_datasource</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">schema_context</span><span class="o">=</span><span class="n">SURVEY_SCHEMA</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="n">check_schema</span> <span class="o">&gt;&gt;</span> <span class="n">generate_sql</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">run_query</span> <span class="o">=</span> <span class="n">AnalyticsOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"run_query"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">datasource_configs</span><span class="o">=</span><span class="p">[</span><span class="n">survey_datasource</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">        <span class="n">queries</span><span class="o">=</span><span class="p">[</span><span class="s2">"{{ ti.xcom_pull(task_ids='generate_sql') }}"</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">        <span class="n">result_output_format</span><span class="o">=</span><span class="s2">"json"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nd">@task</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">extract_data</span><span class="p">(</span><span class="n">raw</span><span class="p">:</span> <span class="nb">str</span><span class="p">)</span> <span class="o">-&gt;</span> <span class="nb">str</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">        <span class="n">results</span> <span class="o">=</span> <span class="n">json</span><span class="o">.</span><span class="n">loads</span><span class="p">(</span><span class="n">raw</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="n">data</span> <span class="o">=</span> <span class="p">[</span><span class="n">row</span> <span class="k">for</span> <span class="n">item</span> <span class="ow">in</span> <span class="n">results</span> <span class="k">for</span> <span class="n">row</span> <span class="ow">in</span> <span class="n">item</span><span class="p">[</span><span class="s2">"data"</span><span class="p">]]</span>
</span></span><span class="line"><span class="cl">        <span class="k">return</span> <span class="n">json</span><span class="o">.</span><span class="n">dumps</span><span class="p">(</span><span class="n">data</span><span class="p">,</span> <span class="n">indent</span><span class="o">=</span><span class="mi">2</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">result_data</span> <span class="o">=</span> <span class="n">extract_data</span><span class="p">(</span><span class="n">run_query</span><span class="o">.</span><span class="n">output</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nd">@task</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">send_result</span><span class="p">(</span><span class="n">data</span><span class="p">:</span> <span class="nb">str</span><span class="p">)</span> <span class="o">-&gt;</span> <span class="kc">None</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">        <span class="k">if</span> <span class="n">SMTP_CONN_ID</span> <span class="ow">and</span> <span class="n">NOTIFY_EMAIL</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">            <span class="kn">from</span> <span class="nn">airflow.providers.smtp.hooks.smtp</span> <span class="kn">import</span> <span class="n">SmtpHook</span>
</span></span><span class="line"><span class="cl">            <span class="k">with</span> <span class="n">SmtpHook</span><span class="p">(</span><span class="n">smtp_conn_id</span><span class="o">=</span><span class="n">SMTP_CONN_ID</span><span class="p">)</span> <span class="k">as</span> <span class="n">hook</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">                <span class="n">hook</span><span class="o">.</span><span class="n">send_email_smtp</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">                    <span class="n">to</span><span class="o">=</span><span class="n">NOTIFY_EMAIL</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">                    <span class="n">subject</span><span class="o">=</span><span class="sa">f</span><span class="s2">"Airflow Survey Analysis: </span><span class="si">{</span><span class="n">SCHEDULED_PROMPT</span><span class="si">}</span><span class="s2">"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">                    <span class="n">html_content</span><span class="o">=</span><span class="sa">f</span><span class="s2">"&lt;pre&gt;</span><span class="si">{</span><span class="n">data</span><span class="si">}</span><span class="s2">&lt;/pre&gt;"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">                <span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">else</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">            <span class="nb">print</span><span class="p">(</span><span class="sa">f</span><span class="s2">"Survey analysis result:</span><span class="se">\n</span><span class="si">{</span><span class="n">data</span><span class="si">}</span><span class="s2">"</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">generate_sql</span> <span class="o">&gt;&gt;</span> <span class="n">run_query</span> <span class="o">&gt;&gt;</span> <span class="n">result_data</span> <span class="o">&gt;&gt;</span> <span class="n">send_result</span><span class="p">(</span><span class="n">result_data</span><span class="p">)</span>
</span></span></code></pre></div><h2 id="connecting-your-llm">Connecting Your LLM</h2>
<p>Both DAGs use <code>llm_conn_id=&quot;pydanticai_default&quot;</code>. Create a connection in the Airflow UI:</p>
<table>
  <thead>
      <tr>
          <th>Provider</th>
          <th>Connection type</th>
          <th>Required fields</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>OpenAI</td>
          <td><code>pydanticai</code></td>
          <td>Password: API key. Extra: <code>{&quot;model&quot;: &quot;openai:gpt-4o&quot;}</code></td>
      </tr>
      <tr>
          <td>Anthropic</td>
          <td><code>pydanticai</code></td>
          <td>Password: API key. Extra: <code>{&quot;model&quot;: &quot;anthropic:claude-haiku-4-5-20251001&quot;}</code></td>
      </tr>
      <tr>
          <td>Google Vertex</td>
          <td><code>pydanticai-vertex</code></td>
          <td>Extra: <code>{&quot;model&quot;: &quot;google-vertex:gemini-2.0-flash&quot;, &quot;project&quot;: &quot;...&quot;, &quot;vertexai&quot;: true}</code></td>
      </tr>
      <tr>
          <td>AWS Bedrock</td>
          <td><code>pydanticai-bedrock</code></td>
          <td>Extra: <code>{&quot;model&quot;: &quot;bedrock:us.anthropic.claude-opus-4-5&quot;, &quot;region_name&quot;: &quot;us-east-1&quot;}</code></td>
      </tr>
  </tbody>
</table>
<p>Switch providers by changing the connection. Neither DAG requires any code changes.</p>
<p>For the scheduled DAG, also create an HTTP connection named <code>airflow_website</code> with host
<code>https://airflow.apache.org</code> (no auth required), and optionally set the <code>SMTP_CONN_ID</code>
and <code>NOTIFY_EMAIL</code> environment variables to enable email delivery.</p>
<h2 id="what-this-shows">What This Shows</h2>
<p>Four capabilities come together across these two pipelines that haven&rsquo;t been easy to combine before:</p>
<p><strong>Natural language as the interface.</strong> Neither pipeline requires the analyst to write SQL.
<code>LLMSQLQueryOperator</code> handles schema awareness, column quoting, and query structure. The
<code>SURVEY_SCHEMA</code> context is the only thing it needs.</p>
<p><strong>In-process query execution.</strong> <code>AnalyticsOperator</code> runs Apache DataFusion inside the Airflow
worker. There&rsquo;s no database to configure, no connection to manage for the data itself. Point
it at a file URI and it runs.</p>
<p><strong>Schema-aware data validation.</strong> <code>LLMSchemaCompareOperator</code> uses an LLM to compare schemas
and surface structural changes in plain language, not a column count diff, but an explanation
of what changed and why it matters for downstream queries. It turns a silent mid-pipeline
failure into an early, actionable error.</p>
<p><strong>Human oversight without blocking automation.</strong> The <code>HITLEntryOperator</code> and <code>ApprovalOperator</code>
are standard Airflow operators from <code>airflow.providers.standard.operators.hitl</code>. They have no
AI imports. They just pause the DAG and wait. The interactive pipeline uses them at both ends;
the scheduled pipeline skips them entirely. Adding or removing human review requires no changes
to the LLM or DataFusion steps.</p>
<h2 id="try-it">Try It</h2>
<p>Both DAGs are in the <code>common.ai</code> provider example DAGs:
<a href="https://github.com/apache/airflow/tree/main/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_analysis.py"><code>example_llm_survey_analysis.py</code></a>.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-bash"><span class="line"><span class="cl">pip install <span class="s1">'apache-airflow-providers-common-ai'</span> <span class="se">\
</span></span></span><span class="line"><span class="cl"><span class="se"></span>            <span class="s1">'apache-airflow-providers-common-sql[datafusion]'</span> <span class="se">\
</span></span></span><span class="line"><span class="cl"><span class="se"></span>            <span class="s1">'apache-airflow-providers-http'</span> <span class="se">\
</span></span></span><span class="line"><span class="cl"><span class="se"></span>            <span class="s1">'apache-airflow-providers-smtp'</span>   <span class="c1"># optional, for email delivery</span>
</span></span></code></pre></div><p>Requires Apache Airflow 3.0+. <code>apache-airflow-providers-standard</code> (which provides
<code>HITLEntryOperator</code> and <code>ApprovalOperator</code>) ships with Airflow 3 and does not need
a separate install.</p>
<p>For the interactive DAG: set <code>SURVEY_CSV_PATH</code> to your local copy of the survey CSV, create
a <code>pydanticai_default</code> connection, and trigger <code>example_llm_survey_interactive</code>.</p>
<p>For the scheduled DAG: create the <code>airflow_website</code> HTTP connection, set <code>SMTP_CONN_ID</code> and
<code>NOTIFY_EMAIL</code> if you want email delivery, and trigger <code>example_llm_survey_scheduled</code>.</p>
<p>To go further, the follow-on post <a href="https://airflow.apache.org/blog/agentic-workloads-airflow-3/">Agentic Workloads on Airflow 3</a>
extends this example into a multi-query synthesis pattern, answering questions that require
querying several dimensions in parallel and synthesizing the results with a second LLM call.</p>
<p>Questions, feedback, and survey queries that stumped the LLM are all welcome on
<a href="https://s.apache.org/airflow-slack">Airflow Slack</a> in <code>#airflow-ai</code>.</p>

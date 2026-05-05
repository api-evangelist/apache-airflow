---
title: "Agentic Workloads on Airflow: Observable, Retryable, and Auditable by Design"
url: "https://airflow.apache.org/blog/agentic-workloads-airflow-3/"
date: "2026-04-15T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>A question like &ldquo;How does AI tool usage vary across Airflow versions?&rdquo; has a natural SQL shape: one cross-tabulation, one result. A question like &ldquo;What does a typical Airflow deployment look like for practitioners who are actively using AI in their workflow?&rdquo; does not. It requires querying executor type, deployment method, cloud provider, and Airflow version independently, each filtered to the same respondent group, then synthesizing the results into a coherent picture. No single query returns the answer. The answer emerges from the relationship between all of them.</p>
<p>This is where Airflow&rsquo;s agentic pattern begins: not when you add an LLM to a workflow, but when the structure of the work itself depends on running multiple LLM calls whose outputs feed a synthesis step. This post builds that pattern using the <a href="https://airflow.apache.org/survey/">2025 Airflow Community Survey</a> data set and the <a href="https://airflow.apache.org/blog/common-ai-provider/"><code>apache-airflow-providers-common-ai</code></a> provider for Airflow 3.</p>
<p>If you haven&rsquo;t read the <a href="https://airflow.apache.org/blog/ai-survey-analysis-pipelines/">introductory survey analysis post</a> yet, start there for a walkthrough of the single-query interactive and scheduled pipelines. This post picks up where that one ends.</p>
<h2 id="the-agentic-gap-in-the-single-query-pattern">The Agentic Gap in the Single-Query Pattern</h2>
<p>The interactive and scheduled survey DAGs from the introductory post each do one thing: translate a natural language question into SQL, execute it against the CSV, and return the result. The LLM is involved once. The structure of the pipeline does not change based on what that LLM call returns.</p>
<p>That is not a limitation to fix. It is the right design for that class of question. For a large fraction of production AI workflows, a single well-structured LLM call with good context is sufficient and preferable.</p>
<p>The pattern becomes agentic when two things are true simultaneously:</p>
<ol>
<li>The question requires querying multiple independent dimensions</li>
<li>The synthesis step, the thing that produces the final answer, depends on <em>all</em> of those results</li>
</ol>
<p>In an agent harness framework, this would be handled inside a reasoning loop: the LLM decides to call a tool, receives a result, calls another tool, accumulates context, and eventually produces a synthesis. Each tool call is invisible to any outside observer. If one tool call fails, the loop either retries internally or fails entirely.</p>
<p>In Airflow, the same logic takes a different shape. Each sub-query becomes a named task. The fan-out is Dynamic Task Mapping. The synthesis is a named task with its inputs in XCom. Every step is observable, independently retryable, and logged.</p>
<h2 id="the-dag-example_llm_survey_agentic">The DAG: <code>example_llm_survey_agentic</code></h2>
<p>The full DAG is in
<a href="https://github.com/apache/airflow/tree/main/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_agentic.py"><code>example_llm_survey_agentic.py</code></a>.</p>
<p><strong>Question:</strong> <em>&ldquo;What does a typical Airflow deployment look like for practitioners who actively use AI tools in their workflow?&rdquo;</em></p>
<p><strong>Task graph:</strong></p>
<pre tabindex="0"><code>decompose_question  →  generate_sql (×4)  →  wrap_query (×4)  →  run_query (×4)
   (@task)              (LLMSQLQuery,          (@task,              (Analytics,
                         mapped)                mapped)              mapped)
                                                                         ↓
                                                                  collect_results
                                                                     (@task)
                                                                         ↓
                                                                  synthesize_answer
                                                                    (LLMOperator)
                                                                         ↓
                                                                  result_confirmation
                                                                   (ApprovalOperator)
</code></pre><p>Seven tasks. Four of them run in parallel. Two LLM calls total: one for SQL generation (four instances), one for synthesis. One human review at the end.</p>
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
          <td><code>@task decompose_question</code></td>
          <td>Returns a list of four sub-questions, one per dimension.</td>
      </tr>
      <tr>
          <td>2</td>
          <td><code>LLMSQLQueryOperator</code> (mapped ×4)</td>
          <td>Each sub-question becomes one SQL query, translated and validated in parallel.</td>
      </tr>
      <tr>
          <td>3</td>
          <td><code>@task wrap_query</code> (mapped ×4)</td>
          <td>Wraps each SQL string into a single-element list for <code>AnalyticsOperator</code>.</td>
      </tr>
      <tr>
          <td>4</td>
          <td><code>AnalyticsOperator</code> (mapped ×4)</td>
          <td>Apache DataFusion executes all four queries in parallel against the local CSV.</td>
      </tr>
      <tr>
          <td>5</td>
          <td><code>@task collect_results</code></td>
          <td>Gathers the four JSON results and labels each by dimension.</td>
      </tr>
      <tr>
          <td>6</td>
          <td><code>LLMOperator</code></td>
          <td>Reads all four labeled result sets and writes a narrative characterization.</td>
      </tr>
      <tr>
          <td>7</td>
          <td><code>ApprovalOperator</code></td>
          <td>Human reviews the synthesized narrative before the DAG completes.</td>
      </tr>
  </tbody>
</table>
<h2 id="decomposing-the-question">Decomposing the Question</h2>
<p><code>decompose_question</code> is a plain <code>@task</code> that returns the list of sub-questions. In this example, the list is static: the four dimensions are hardcoded as strings:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="nd">@task</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">decompose_question</span><span class="p">()</span> <span class="o">-&gt;</span> <span class="nb">list</span><span class="p">[</span><span class="nb">str</span><span class="p">]:</span>
</span></span><span class="line"><span class="cl">    <span class="k">return</span> <span class="p">[</span>
</span></span><span class="line"><span class="cl">        <span class="s2">"""
</span></span></span><span class="line"><span class="cl"><span class="s2">Among respondents who use AI/LLM tools to write Airflow code,
</span></span></span><span class="line"><span class="cl"><span class="s2">what executor types (CeleryExecutor, KubernetesExecutor, LocalExecutor)
</span></span></span><span class="line"><span class="cl"><span class="s2">are most commonly enabled? Count an executor as enabled only if the
</span></span></span><span class="line"><span class="cl"><span class="s2">column value is clearly affirmative. Treat blank, NULL, and negative
</span></span></span><span class="line"><span class="cl"><span class="s2">values as not enabled. Return the count per executor type."""</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="s2">"""
</span></span></span><span class="line"><span class="cl"><span class="s2">Among respondents who use AI/LLM tools to write Airflow code,
</span></span></span><span class="line"><span class="cl"><span class="s2">how do they deploy Airflow? Return the count per deployment method."""</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="s2">"""
</span></span></span><span class="line"><span class="cl"><span class="s2">Among respondents who use AI/LLM tools to write Airflow code,
</span></span></span><span class="line"><span class="cl"><span class="s2">which cloud providers are most commonly used for Airflow?
</span></span></span><span class="line"><span class="cl"><span class="s2">Return the count per cloud provider."""</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="s2">"""
</span></span></span><span class="line"><span class="cl"><span class="s2">Among respondents who use AI/LLM tools to write Airflow code,
</span></span></span><span class="line"><span class="cl"><span class="s2">what version of Airflow are they currently running?
</span></span></span><span class="line"><span class="cl"><span class="s2">Return the count per version."""</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">]</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">sub_questions</span> <span class="o">=</span> <span class="n">decompose_question</span><span class="p">()</span>
</span></span></code></pre></div><p>The output of this task, a list of four strings, becomes the input to the <code>expand()</code> call on <code>LLMSQLQueryOperator</code> in the next step. Airflow creates one mapped task instance per list element.</p>
<blockquote>
<p><strong>Why keep this static?</strong> A dynamic version, where the LLM itself decomposes the high-level question into sub-questions at runtime, is possible and more agentic. It adds an LLM call before any SQL runs, which introduces latency and a failure point early in the graph. For a first example, static decomposition is clearer. The dynamic variant is a follow-on pattern.</p></blockquote>
<h2 id="sql-generation-mapping-over-sub-questions">SQL Generation: Mapping Over Sub-Questions</h2>
<p><code>LLMSQLQueryOperator.partial().expand()</code> creates one mapped task instance per sub-question. All four run in parallel, each translating one natural language question into validated SQL against the survey schema:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="n">generate_sql</span> <span class="o">=</span> <span class="n">LLMSQLQueryOperator</span><span class="o">.</span><span class="n">partial</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">task_id</span><span class="o">=</span><span class="s2">"generate_sql"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">llm_conn_id</span><span class="o">=</span><span class="n">LLM_CONN_ID</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">datasource_config</span><span class="o">=</span><span class="n">survey_datasource</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">schema_context</span><span class="o">=</span><span class="n">SURVEY_SCHEMA</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span><span class="o">.</span><span class="n">expand</span><span class="p">(</span><span class="n">prompt</span><span class="o">=</span><span class="n">sub_questions</span><span class="p">)</span>
</span></span></code></pre></div><p>In the Airflow UI, this renders as four task instances: <code>generate_sql[0]</code>, <code>generate_sql[1]</code>, <code>generate_sql[2]</code>, <code>generate_sql[3]</code>. Each has its own log, retry counter, and XCom entry. This is what an agent harness&rsquo;s parallel tool calls look like when they are made explicit.</p>
<p>Each instance returns a single SQL string. <code>LLMSQLQueryOperator</code> validates the output with sqlglot before returning it. Anything that is not a <code>SELECT</code> statement is rejected.</p>
<h2 id="the-wrap_query-bridge">The <code>wrap_query</code> Bridge</h2>
<p><code>AnalyticsOperator</code> expects <code>queries: list[str]</code>, a list because it can run multiple queries in one execution. <code>LLMSQLQueryOperator</code> returns a single <code>str</code>. A small <code>@task</code> bridges the interface:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="nd">@task</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">wrap_query</span><span class="p">(</span><span class="n">sql</span><span class="p">:</span> <span class="nb">str</span><span class="p">)</span> <span class="o">-&gt;</span> <span class="nb">list</span><span class="p">[</span><span class="nb">str</span><span class="p">]:</span>
</span></span><span class="line"><span class="cl">    <span class="k">return</span> <span class="p">[</span><span class="n">sql</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">wrapped_queries</span> <span class="o">=</span> <span class="n">wrap_query</span><span class="o">.</span><span class="n">expand</span><span class="p">(</span><span class="n">sql</span><span class="o">=</span><span class="n">generate_sql</span><span class="o">.</span><span class="n">output</span><span class="p">)</span>
</span></span></code></pre></div><p>This step is an implementation detail, not a conceptual one. Four mapped instances of <code>wrap_query</code> run in parallel, each converting one SQL string into a one-element list. The result is four <code>list[str]</code> values that <code>AnalyticsOperator</code> can consume directly.</p>
<h2 id="parallel-execution-via-datafusion">Parallel Execution via DataFusion</h2>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="n">run_query</span> <span class="o">=</span> <span class="n">AnalyticsOperator</span><span class="o">.</span><span class="n">partial</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">task_id</span><span class="o">=</span><span class="s2">"run_query"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">datasource_configs</span><span class="o">=</span><span class="p">[</span><span class="n">survey_datasource</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">    <span class="n">result_output_format</span><span class="o">=</span><span class="s2">"json"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span><span class="o">.</span><span class="n">expand</span><span class="p">(</span><span class="n">queries</span><span class="o">=</span><span class="n">wrapped_queries</span><span class="p">)</span>
</span></span></code></pre></div><p>Four mapped instances of <code>AnalyticsOperator</code> run in parallel. Each loads the survey CSV into an Apache DataFusion <code>SessionContext</code> in-process and executes its SQL against it. No database server, no shared state between instances.</p>
<p>This is where independent retry earns its value. If the cloud provider query returns a DataFusion error due to a null value in that column, only <code>run_query[2]</code> fails. <code>run_query[0]</code>, <code>run_query[1]</code>, and <code>run_query[3]</code> have already succeeded and their results are in XCom. When <code>run_query[2]</code> is cleared and retried, the other three results are preserved.</p>
<h2 id="collecting-and-labeling-results">Collecting and Labeling Results</h2>
<p><code>collect_results</code> gathers all four outputs from <code>run_query</code> (Airflow passes the list of mapped outputs directly) and labels each one by dimension key:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="c1"># DIMENSION_KEYS = ["executor", "deployment", "cloud", "airflow_version"]</span>
</span></span><span class="line"><span class="cl"><span class="c1"># Order must match the sub-questions returned by decompose_question.</span>
</span></span><span class="line"><span class="cl"><span class="c1"># Airflow preserves mapped task output ordering by index, so this zip is safe.</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="nd">@task</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">collect_results</span><span class="p">(</span><span class="n">results</span><span class="p">:</span> <span class="nb">list</span><span class="p">[</span><span class="nb">str</span><span class="p">])</span> <span class="o">-&gt;</span> <span class="nb">dict</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">    <span class="n">labeled</span><span class="p">:</span> <span class="nb">dict</span><span class="p">[</span><span class="nb">str</span><span class="p">,</span> <span class="nb">list</span><span class="p">]</span> <span class="o">=</span> <span class="p">{}</span>
</span></span><span class="line"><span class="cl">    <span class="k">for</span> <span class="n">key</span><span class="p">,</span> <span class="n">raw</span> <span class="ow">in</span> <span class="nb">zip</span><span class="p">(</span><span class="n">DIMENSION_KEYS</span><span class="p">,</span> <span class="n">results</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">        <span class="n">items</span> <span class="o">=</span> <span class="n">json</span><span class="o">.</span><span class="n">loads</span><span class="p">(</span><span class="n">raw</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="n">data</span> <span class="o">=</span> <span class="p">[</span><span class="n">row</span> <span class="k">for</span> <span class="n">item</span> <span class="ow">in</span> <span class="n">items</span> <span class="k">for</span> <span class="n">row</span> <span class="ow">in</span> <span class="n">item</span><span class="p">[</span><span class="s2">"data"</span><span class="p">]]</span>
</span></span><span class="line"><span class="cl">        <span class="n">labeled</span><span class="p">[</span><span class="n">key</span><span class="p">]</span> <span class="o">=</span> <span class="n">data</span>
</span></span><span class="line"><span class="cl">    <span class="k">return</span> <span class="n">labeled</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">collected</span> <span class="o">=</span> <span class="n">collect_results</span><span class="p">(</span><span class="n">run_query</span><span class="o">.</span><span class="n">output</span><span class="p">)</span>
</span></span></code></pre></div><p>The output is a dict like:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"executor"</span><span class="p">:</span> <span class="p">[{</span><span class="nt">"KubernetesExecutor"</span><span class="p">:</span> <span class="s2">"Yes"</span><span class="p">,</span> <span class="nt">"count"</span><span class="p">:</span> <span class="mi">847</span><span class="p">},</span> <span class="err">...</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"deployment"</span><span class="p">:</span> <span class="p">[{</span><span class="nt">"How do you deploy Airflow?"</span><span class="p">:</span> <span class="s2">"Managed Cloud Service"</span><span class="p">,</span> <span class="nt">"count"</span><span class="p">:</span> <span class="mi">1203</span><span class="p">},</span> <span class="err">...</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"cloud"</span><span class="p">:</span> <span class="p">[{</span><span class="nt">"primary_cloud"</span><span class="p">:</span> <span class="s2">"AWS"</span><span class="p">,</span> <span class="nt">"count"</span><span class="p">:</span> <span class="mi">891</span><span class="p">},</span> <span class="err">...</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"airflow_version"</span><span class="p">:</span> <span class="p">[{</span><span class="nt">"version"</span><span class="p">:</span> <span class="s2">"3.x"</span><span class="p">,</span> <span class="nt">"count"</span><span class="p">:</span> <span class="mi">412</span><span class="p">},</span> <span class="err">...</span><span class="p">]</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div><p>All four result sets in one XCom entry. This is the input to the synthesis step.</p>
<h2 id="synthesis-the-second-llm-call">Synthesis: The Second LLM Call</h2>
<p><code>LLMOperator</code> takes the collected results and produces a narrative. This is the synthesis step, the part of the pipeline that could not exist without all four sub-queries having completed first:</p>
<p>The <code>generate_sql</code> step also receives a <code>system_prompt=SQL_SYSTEM_PROMPT</code> that instructs the LLM on quoting conventions, AI usage filter semantics, and how to handle blank/NULL/ambiguous values. That system prompt is defined once at the module level and shared across all four mapped instances.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="n">synthesize_answer</span> <span class="o">=</span> <span class="n">LLMOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">task_id</span><span class="o">=</span><span class="s2">"synthesize_answer"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">llm_conn_id</span><span class="o">=</span><span class="n">LLM_CONN_ID</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">system_prompt</span><span class="o">=</span><span class="n">SYNTHESIS_SYSTEM_PROMPT</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">prompt</span><span class="o">=</span><span class="s2">"""
</span></span></span><span class="line"><span class="cl"><span class="s2">Given these four independent survey query results about practitioners
</span></span></span><span class="line"><span class="cl"><span class="s2">who use AI tools to write Airflow code, write a 2-3 sentence
</span></span></span><span class="line"><span class="cl"><span class="s2">characterization of what a typical Airflow deployment looks like for
</span></span></span><span class="line"><span class="cl"><span class="s2">this group.
</span></span></span><span class="line"><span class="cl"><span class="s2">
</span></span></span><span class="line"><span class="cl"><span class="s2">Results: {{ ti.xcom_pull(task_ids='collect_results') }}"""</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="n">collected</span> <span class="o">&gt;&gt;</span> <span class="n">synthesize_answer</span>
</span></span></code></pre></div><p><code>prompt</code> is a template field, so <code>{{ ti.xcom_pull(task_ids='collect_results') }}</code> renders the full dict at execution time. <code>system_prompt</code> maps to the PydanticAI agent&rsquo;s <code>instructions</code> parameter, so the framing instruction carries into every token the model generates.</p>
<p>The output, a 2-3 sentence characterization, goes to XCom and then to the final approval step.</p>
<blockquote>
<p><strong>Inline HITL alternative:</strong> <code>LLMOperator</code> supports <code>require_approval=True</code> and <code>allow_modifications=True</code> as constructor parameters, via <code>LLMApprovalMixin</code>. Setting these eliminates the separate <code>ApprovalOperator</code> task and lets the reviewer edit the synthesized narrative directly before approving. Whether to use inline approval or a separate <code>ApprovalOperator</code> is a design choice; both produce the same result.</p></blockquote>
<h2 id="walking-through-a-run">Walking Through a Run</h2>
<p><strong>Step 1: Decompose.</strong> Trigger the DAG. <code>decompose_question</code> completes in milliseconds and returns the four sub-question strings.</p>
<p><strong>Steps 2–4: Fan-out.</strong> Twelve mapped task instances run: four <code>generate_sql</code>, four <code>wrap_query</code>, four <code>run_query</code>. In the Airflow UI, these appear as three rows of four parallel task instances. Each SQL generation call goes to the LLM; each DataFusion execution runs in-process against the CSV.</p>
<figure><img alt="generate_sql[3] task logs showing the LLM-generated SQL query" src="/blog/agentic-workloads-airflow-3/images/agentic-generate-sql-logs.png" /><figcaption>
      <p>Each mapped generate_sql instance has its own log showing the prompt, generated SQL, and sqlglot validation.</p>
    </figcaption>
</figure>

<figure><img alt="collect_results XCom showing labeled dimension data" src="/blog/agentic-workloads-airflow-3/images/agentic-collect-results-xcom.png" /><figcaption>
      <p>The collect_results task labels each sub-query result by dimension key. All four result sets are visible in XCom.</p>
    </figcaption>
</figure>

<figure><img alt="ApprovalOperator showing the synthesized narrative" src="/blog/agentic-workloads-airflow-3/images/agentic-approval-dialog.png" /><figcaption>
      <p>The final ApprovalOperator presents the LLM-synthesized narrative for human review.</p>
    </figcaption>
</figure>

<p>A representative generated query for the executor dimension:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-sql"><span class="line"><span class="cl"><span class="k">SELECT</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="k">CASE</span><span class="w"> </span><span class="k">WHEN</span><span class="w"> </span><span class="s2">"CeleryExecutor"</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="s1">'Yes'</span><span class="w"> </span><span class="k">THEN</span><span class="w"> </span><span class="s1">'CeleryExecutor'</span><span class="w"> </span><span class="k">END</span><span class="w">        </span><span class="k">AS</span><span class="w"> </span><span class="n">executor_type</span><span class="p">,</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="k">COUNT</span><span class="p">(</span><span class="o">*</span><span class="p">)</span><span class="w"> </span><span class="k">AS</span><span class="w"> </span><span class="k">count</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">FROM</span><span class="w"> </span><span class="n">survey</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">WHERE</span><span class="w"> </span><span class="s2">"Are you using AI/LLM (ChatGPT/Cursor/Claude etc) to assist you in writing Airflow code?"</span><span class="w"> </span><span class="o">!=</span><span class="w"> </span><span class="s1">'No, I don''t use AI to write Airflow code'</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">  </span><span class="k">AND</span><span class="w"> </span><span class="s2">"CeleryExecutor"</span><span class="w"> </span><span class="k">IS</span><span class="w"> </span><span class="k">NOT</span><span class="w"> </span><span class="k">NULL</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">GROUP</span><span class="w"> </span><span class="k">BY</span><span class="w"> </span><span class="n">executor_type</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">UNION</span><span class="w"> </span><span class="k">ALL</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">SELECT</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="k">CASE</span><span class="w"> </span><span class="k">WHEN</span><span class="w"> </span><span class="s2">"KubernetesExecutor"</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="s1">'Yes'</span><span class="w"> </span><span class="k">THEN</span><span class="w"> </span><span class="s1">'KubernetesExecutor'</span><span class="w"> </span><span class="k">END</span><span class="p">,</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="k">COUNT</span><span class="p">(</span><span class="o">*</span><span class="p">)</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">FROM</span><span class="w"> </span><span class="n">survey</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">WHERE</span><span class="w"> </span><span class="s2">"Are you using AI/LLM (ChatGPT/Cursor/Claude etc) to assist you in writing Airflow code?"</span><span class="w"> </span><span class="o">!=</span><span class="w"> </span><span class="s1">'No, I don''t use AI to write Airflow code'</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">  </span><span class="k">AND</span><span class="w"> </span><span class="s2">"KubernetesExecutor"</span><span class="w"> </span><span class="k">IS</span><span class="w"> </span><span class="k">NOT</span><span class="w"> </span><span class="k">NULL</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="k">GROUP</span><span class="w"> </span><span class="k">BY</span><span class="w"> </span><span class="mi">1</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w"></span><span class="c1">-- ... and so on
</span></span></span></code></pre></div><p><strong>Step 5: Collect.</strong> <code>collect_results</code> assembles the four result sets into a labeled dict.</p>
<p><strong>Step 6: Synthesize.</strong> <code>LLMOperator</code> calls the LLM once with all four result sets as context. A representative output:</p>
<blockquote>
<p>&ldquo;Among practitioners who actively use AI tools to write Airflow code, the majority (61%) deploy on a managed cloud service or cloud-native setup, with AWS as the primary cloud provider (38%). KubernetesExecutor is the dominant choice (54%), and this group is adopting Airflow 3.x at a notably higher rate than the survey population as a whole (29% vs. 21% overall).&rdquo;</p></blockquote>
<p><strong>Step 7: Review.</strong> The <code>ApprovalOperator</code> presents the narrative in the Airflow UI. Approve to complete the DAG; reject to fail it and trigger a retry from the synthesis step if desired.</p>
<h2 id="what-the-dag-topology-makes-explicit">What the DAG Topology Makes Explicit</h2>
<p>The core difference between this pattern and the equivalent agent harness implementation is not the output. It is what is auditable after the run.</p>
<table>
  <thead>
      <tr>
          <th>What&rsquo;s happening</th>
          <th>In an agent harness</th>
          <th>In this DAG</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>Sub-query: executor distribution</td>
          <td>LLM internal tool call, no external artifact</td>
          <td>Task <code>generate_sql[0]</code>: SQL in XCom, full log</td>
      </tr>
      <tr>
          <td>Sub-query: cloud provider</td>
          <td>LLM internal tool call</td>
          <td>Task <code>generate_sql[2]</code>: SQL in XCom, full log</td>
      </tr>
      <tr>
          <td>Parallel execution</td>
          <td>Concurrent or sequential, implementation-dependent</td>
          <td>Explicit mapped instances, each on its own worker</td>
      </tr>
      <tr>
          <td>cloud_provider query fails</td>
          <td>Entire run restarts from the top, or fails</td>
          <td>Only <code>run_query[2]</code> retries; other three results preserved</td>
      </tr>
      <tr>
          <td>Synthesis inputs</td>
          <td>Accumulated context in the LLM&rsquo;s reasoning loop</td>
          <td><code>collect_results</code> XCom entry: the exact dict the LLM received</td>
      </tr>
      <tr>
          <td>Why did it characterize it that way?</td>
          <td>No artifact</td>
          <td><code>synthesize_answer</code> XCom: input dict and output string both stored</td>
      </tr>
  </tbody>
</table>
<p>Each <code>generate_sql[i]</code> task log contains the prompt the LLM received, the SQL it returned, and the validation result from sqlglot. Each <code>run_query[i]</code> log contains the DataFusion execution details and the row count returned. The synthesis step&rsquo;s XCom entry contains the exact dict that was passed as context.</p>
<p>This is the same information an agent harness has internally. The difference is that Airflow surfaces it as first-class task artifacts, accessible from the Airflow UI without instrumenting or patching the reasoning loop.</p>
<h2 id="connecting-your-llm">Connecting Your LLM</h2>
<p>Both <code>LLMSQLQueryOperator</code> and <code>LLMOperator</code> use <code>llm_conn_id=&quot;pydanticai_default&quot;</code>. The same connection table from the introductory post applies:</p>
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
<p>One connection serves both operators. The synthesis step and the SQL generation steps can use different connections if you want a stronger model for synthesis and a faster one for the SQL generation pass. Set <code>model_id</code> on the <code>LLMOperator</code> to override the connection&rsquo;s default.</p>
<h2 id="the-multi-agent-pattern-hidden-in-plain-sight">The Multi-Agent Pattern Hidden in Plain Sight</h2>
<p>This DAG was not designed around multi-agent frameworks, but it accidentally implements one of the most common separation-of-concerns patterns in that space: the <strong>SQL Architect / Critic / Narrator</strong> triad.</p>
<p>In agent harness frameworks, these three roles are typically implemented as distinct agent instances that coordinate through an internal routing layer. The underlying rationale is that mixing generation, evaluation, and communication into a single agent produces outputs that are mediocre at all three jobs. Separating them forces each role to reason only about what it is responsible for.</p>
<p>The survey DAG lands in the same place through a different path: the task boundary enforces the separation.</p>
<p><strong>SQL Architect → <code>generate_sql[0..3]</code> (<code>LLMSQLQueryOperator</code>).</strong>
Each mapped instance receives one natural language sub-question and produces one SQL query. Schema context is passed as a system-level framing, not as part of the user prompt, so the model reasons about structure before generating syntax. The Architect role is strict: produce a valid <code>SELECT</code> statement or fail.</p>
<p><strong>Critic → two layers.</strong>
The first layer is embedded in <code>LLMSQLQueryOperator</code>: sqlglot parses and validates the generated SQL before the task returns. This is a syntax-level Critic: it rejects anything that is not a <code>SELECT</code>. The second and fuller layer is the <code>LLMBranchOperator</code> pattern: an explicit task that evaluates result quality and decides whether the finding is reportable, needs a drill-down, or warrants a pivot to a different hypothesis. That task does what the Critic does in a multi-agent system. It challenges the output rather than accepting it.</p>
<p><strong>Narrator → <code>synthesize_answer</code> (<code>LLMOperator</code>).</strong>
Receives the labeled result sets from all four dimensions and produces a plain-language characterization. The Narrator&rsquo;s role is bounded by design: it receives structured data rows, not the intermediate SQL or any reasoning artifacts, and its system prompt constrains it to communication: &ldquo;focus on patterns and proportions rather than raw counts.&rdquo; The role separation is enforced by what is in XCom, not by agent routing logic.</p>
<p>One genuine structural difference remains. In a multi-agent system, the Critic can loop back to the Architect with feedback (&ldquo;this query has a NULL handling problem, try again&rdquo;) and the cycle runs until the output meets a quality bar. Airflow DAGs are acyclic. The Critic either raises an exception and triggers a task-level retry of the Architect instance (automatic but blunt), or routes to an alternative path via <code>LLMBranchOperator</code> (explicit and auditable, but the alternative path must be wired in at authoring time). Neither is a pure generative feedback loop.</p>
<p>That acyclicity is a deliberate tradeoff: it is also what makes the DAG&rsquo;s execution fully auditable and its failure modes predictable. The feedback loop pattern, and the open question of how far it can be supported within a structured workflow model, is part of what Airflow&rsquo;s roadmap is actively working through.</p>
<hr />
<h2 id="try-it">Try It</h2>
<p>The DAG is in the <code>common.ai</code> provider example DAGs:
<a href="https://github.com/apache/airflow/tree/main/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_agentic.py"><code>example_llm_survey_agentic.py</code></a>.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-bash"><span class="line"><span class="cl">pip install <span class="s1">'apache-airflow-providers-common-ai'</span> <span class="se">\
</span></span></span><span class="line"><span class="cl"><span class="se"></span>            <span class="s1">'apache-airflow-providers-common-sql[datafusion]'</span>
</span></span></code></pre></div><p>Requires Apache Airflow 3.0+.</p>
<p>Set <code>SURVEY_CSV_PATH</code> to your local cleaned copy of the survey CSV, create a <code>pydanticai_default</code> connection, and trigger <code>example_llm_survey_agentic</code>.</p>
<p>The Airflow UI will show the four parallel <code>generate_sql</code> and <code>run_query</code> instances fanning out and converging to <code>collect_results</code>. That visual is the clearest way to see what distinguishes the agentic pattern from a single-query run.</p>
<p>Questions, results, and sub-questions that surprised the LLM are welcome on <a href="https://s.apache.org/airflow-slack">Airflow Slack</a> in <code>#airflow-ai</code>.</p>

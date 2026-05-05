---
title: "Introducing the Common AI Provider: LLM and AI Agent Support for Apache Airflow"
url: "https://airflow.apache.org/blog/common-ai-provider/"
date: "2026-04-14T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>At <a href="https://airflowsummit.org/sessions/2025/airflow-as-an-ai-agents-toolkit-unlocking-1000-integrations-with-mcp/">Airflow Summit 2025</a>, we previewed what native AI integration in Apache Airflow could look like. Today we&rsquo;re shipping it.</p>
<p><strong><a href="https://pypi.org/project/apache-airflow-providers-common-ai/"><code>apache-airflow-providers-common-ai</code></a> 0.1.0</strong> adds LLM and agent capabilities directly to Airflow. Not a wrapper around another framework, but a provider package that plugs into the orchestrator you already run. It&rsquo;s built on <a href="https://ai.pydantic.dev/">Pydantic AI</a> and supports 20+ model providers (OpenAI, Anthropic, Google, Azure, Bedrock, Ollama, and more) through a single install.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-bash"><span class="line"><span class="cl">pip install <span class="s1">'apache-airflow-providers-common-ai'</span>
</span></span></code></pre></div><p>Requires Apache Airflow 3.0+.</p>
<blockquote>
<p><strong>Note:</strong> This is a 0.x release. We&rsquo;re actively looking for feedback and iterating fast, so breaking changes are possible between minor versions. Try it, tell us what works and what doesn&rsquo;t. Your input directly shapes the API.</p></blockquote>
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
          <td><strong>6</strong></td>
          <td>Operators</td>
      </tr>
      <tr>
          <td><strong>6</strong></td>
          <td>TaskFlow decorators</td>
      </tr>
      <tr>
          <td><strong>5</strong></td>
          <td>Toolsets</td>
      </tr>
      <tr>
          <td><strong>4</strong></td>
          <td>Connection types</td>
      </tr>
      <tr>
          <td><strong>20+</strong></td>
          <td>Supported model providers via Pydantic AI</td>
      </tr>
  </tbody>
</table>
<h2 id="the-decorator-suite">The Decorator Suite</h2>
<p>Every operator has a matching TaskFlow decorator.</p>
<h3 id="taskllm-single-llm-call"><code>@task.llm</code>: Single LLM Call</h3>
<p>Send a prompt, get text or structured output back.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">pydantic</span> <span class="kn">import</span> <span class="n">BaseModel</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.compat.sdk</span> <span class="kn">import</span> <span class="n">dag</span><span class="p">,</span> <span class="n">task</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="nd">@dag</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">my_pipeline</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">    <span class="k">class</span> <span class="nc">Entities</span><span class="p">(</span><span class="n">BaseModel</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">        <span class="n">names</span><span class="p">:</span> <span class="nb">list</span><span class="p">[</span><span class="nb">str</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">        <span class="n">locations</span><span class="p">:</span> <span class="nb">list</span><span class="p">[</span><span class="nb">str</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="nd">@task.llm</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">system_prompt</span><span class="o">=</span><span class="s2">"Extract named entities."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">output_type</span><span class="o">=</span><span class="n">Entities</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">extract</span><span class="p">(</span><span class="n">text</span><span class="p">:</span> <span class="nb">str</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">        <span class="k">return</span> <span class="sa">f</span><span class="s2">"Extract entities from: </span><span class="si">{</span><span class="n">text</span><span class="si">}</span><span class="s2">"</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">extract</span><span class="p">(</span><span class="s2">"Alice visited Paris and met Bob in London."</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">my_pipeline</span><span class="p">()</span>
</span></span></code></pre></div><p>The LLM returns a typed <code>Entities</code> object, not a string you have to parse. Downstream tasks get structured data through <code>XCom</code>.</p>
<h3 id="taskagent-multi-step-agent-with-tools"><code>@task.agent</code>: Multi-Step Agent with Tools</h3>
<p>When the LLM needs to query databases, call APIs, or read files across multiple steps, use <code>@task.agent</code>. The agent picks which tools to call and loops until it has an answer.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.ai.toolsets.sql</span> <span class="kn">import</span> <span class="n">SQLToolset</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.compat.sdk</span> <span class="kn">import</span> <span class="n">dag</span><span class="p">,</span> <span class="n">task</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="nd">@dag</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">sql_analyst</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">    <span class="nd">@task.agent</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">system_prompt</span><span class="o">=</span><span class="s2">"You are a SQL analyst. Use tools to answer questions with data."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">toolsets</span><span class="o">=</span><span class="p">[</span>
</span></span><span class="line"><span class="cl">            <span class="n">SQLToolset</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">                <span class="n">db_conn_id</span><span class="o">=</span><span class="s2">"postgres_default"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">                <span class="n">allowed_tables</span><span class="o">=</span><span class="p">[</span><span class="s2">"customers"</span><span class="p">,</span> <span class="s2">"orders"</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">                <span class="n">max_rows</span><span class="o">=</span><span class="mi">20</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">            <span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="p">],</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">analyze</span><span class="p">(</span><span class="n">question</span><span class="p">:</span> <span class="nb">str</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">        <span class="k">return</span> <span class="sa">f</span><span class="s2">"Answer this question about our data: </span><span class="si">{</span><span class="n">question</span><span class="si">}</span><span class="s2">"</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">analyze</span><span class="p">(</span><span class="s2">"What are the top 5 customers by order count?"</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">sql_analyst</span><span class="p">()</span>
</span></span></code></pre></div><p>Under the hood, the agent calls <code>list_tables</code>, <code>get_schema</code>, and <code>query</code> on its own until it has the answer.</p>
<h3 id="taskllm_branch-llm-powered-branching"><code>@task.llm_branch</code>: LLM-Powered Branching</h3>
<p>The LLM decides which downstream task(s) to run. No string parsing. The LLM returns a constrained enum built from the task&rsquo;s downstream IDs.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="nd">@task.llm_branch</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">system_prompt</span><span class="o">=</span><span class="s2">"Classify the support ticket priority."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">route_ticket</span><span class="p">(</span><span class="n">ticket_text</span><span class="p">:</span> <span class="nb">str</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">    <span class="k">return</span> <span class="sa">f</span><span class="s2">"Classify this ticket: </span><span class="si">{</span><span class="n">ticket_text</span><span class="si">}</span><span class="s2">"</span>
</span></span></code></pre></div><h3 id="taskllm_sql-text-to-sql-with-safety-rails"><code>@task.llm_sql</code>: Text-to-SQL with Safety Rails</h3>
<p>Generates SQL from natural language. The operator introspects your database schema and validates the output via AST parsing (<a href="https://github.com/tobymao/sqlglot">sqlglot</a>) before execution.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.compat.sdk</span> <span class="kn">import</span> <span class="n">dag</span><span class="p">,</span> <span class="n">task</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="nd">@dag</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">sql_generator</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">    <span class="nd">@task.llm_sql</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">db_conn_id</span><span class="o">=</span><span class="s2">"postgres_default"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">table_names</span><span class="o">=</span><span class="p">[</span><span class="s2">"orders"</span><span class="p">,</span> <span class="s2">"customers"</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">        <span class="n">dialect</span><span class="o">=</span><span class="s2">"postgres"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">build_query</span><span class="p">(</span><span class="n">ds</span><span class="o">=</span><span class="kc">None</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">        <span class="k">return</span> <span class="sa">f</span><span class="s2">"Find customers who placed no orders after </span><span class="si">{</span><span class="n">ds</span><span class="si">}</span><span class="s2">"</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">build_query</span><span class="p">()</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">sql_generator</span><span class="p">()</span>
</span></span></code></pre></div><h3 id="taskllm_file_analysis-analyze-files-with-llms"><code>@task.llm_file_analysis</code>: Analyze Files with LLMs</h3>
<p>Point it at files in object storage (S3, GCS, local) and let the LLM analyze them. Supports CSV, Parquet, Avro, JSON, and images (multimodal).</p>
<p><img alt="LLM analyzing a CSV file — identifying columns, counting rows, computing totals" src="/blog/common-ai-provider/images/file-analysis-csv.png" /></p>
<p>It also handles multimodal input. Set <code>multi_modal=True</code> and the operator sends images and PDFs as binary attachments to the LLM.</p>
<h3 id="taskllm_schema_compare-cross-database-schema-drift"><code>@task.llm_schema_compare</code>: Cross-Database Schema Drift</h3>
<p>Compares schemas across databases and returns structured <code>SchemaMismatch</code> results with severity levels. Handles the type mapping headaches across systems (<code>varchar(n)</code> vs <code>string</code>, <code>timestamp</code> vs <code>timestamptz</code>).</p>
<h2 id="350-hooks-as-ai-tools">350+ Hooks as AI Tools</h2>
<p>Airflow already has 350+ provider hooks with typed methods, docstrings, and managed credentials. <code>S3Hook</code>, <code>GCSHook</code>, <code>SlackHook</code>, <code>SnowflakeHook</code>, <code>DbApiHook</code>. They all authenticate through Airflow&rsquo;s secret backend, and they all already work.</p>
<p>Rather than setting up separate MCP servers with their own auth for each integration, <code>HookToolset</code> lets agents call hook methods directly using the connections you&rsquo;ve already configured.</p>
<p><code>HookToolset</code> turns any of them into AI agent tools:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.amazon.aws.hooks.s3</span> <span class="kn">import</span> <span class="n">S3Hook</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.ai.toolsets.hook</span> <span class="kn">import</span> <span class="n">HookToolset</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="c1"># S3Hook methods become agent tools: the agent can list, read, and check S3 objects</span>
</span></span><span class="line"><span class="cl"><span class="n">HookToolset</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">S3Hook</span><span class="p">(</span><span class="n">aws_conn_id</span><span class="o">=</span><span class="s2">"aws_default"</span><span class="p">),</span>
</span></span><span class="line"><span class="cl">    <span class="n">allowed_methods</span><span class="o">=</span><span class="p">[</span><span class="s2">"list_keys"</span><span class="p">,</span> <span class="s2">"read_key"</span><span class="p">,</span> <span class="s2">"check_for_key"</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">    <span class="n">tool_name_prefix</span><span class="o">=</span><span class="s2">"s3_"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span>
</span></span></code></pre></div><p>The introspection engine builds JSON Schema from method signatures and enriches tool descriptions from docstrings (Sphinx and Google style). You explicitly declare which methods to expose. No auto-discovery, no unintended access. The agent sees <code>s3_list_keys</code>, <code>s3_read_key</code>, <code>s3_check_for_key</code> as typed tools with parameter descriptions pulled straight from the hook.</p>
<p>This works with <em>any</em> hook. Want your agent to send Slack messages? <code>HookToolset(SlackHook(...), allowed_methods=[&quot;send_message&quot;])</code>. Query Snowflake? Use <code>SQLToolset</code> with a Snowflake connection. Hit an internal API? <code>HookToolset(HttpHook(...), allowed_methods=[&quot;run&quot;])</code>.</p>
<p>You can also compose multiple toolsets in a single agent. Give it <code>SQLToolset</code> for database access <em>and</em> <code>HookToolset</code> for API calls, and the agent picks the right tool for each step.</p>
<p>Four toolsets ship with the provider:</p>
<table>
  <thead>
      <tr>
          <th>Toolset</th>
          <th>What it does</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><strong><code>SQLToolset</code></strong></td>
          <td><code>list_tables</code>, <code>get_schema</code>, <code>query</code>, <code>check_query</code> for any <code>DbApiHook</code> database</td>
      </tr>
      <tr>
          <td><strong><code>HookToolset</code></strong></td>
          <td>Wraps any Airflow hook&rsquo;s methods as agent tools</td>
      </tr>
      <tr>
          <td><strong><code>MCPToolset</code></strong></td>
          <td>Connects to external <a href="https://modelcontextprotocol.io/">MCP</a> servers via Airflow Connections</td>
      </tr>
      <tr>
          <td><strong><code>DataFusionToolset</code></strong></td>
          <td>SQL over files in object storage (S3, other to come soon) via Apache DataFusion</td>
      </tr>
  </tbody>
</table>
<p>All toolsets resolve connections lazily through <code>BaseHook.get_connection()</code>. No hardcoded keys.</p>
<p>Here&rsquo;s what an agent SQL analysis looks like in the Airflow task logs. The agent explored the schema, wrote queries, and produced a structured summary:</p>
<p><img alt="Agent SQL analysis showing tool calls and structured output in Airflow task logs" src="/blog/common-ai-provider/images/agent-sql-logs.png" /></p>
<h2 id="not-locked-into-decorators">Not Locked Into Decorators</h2>
<p>You don&rsquo;t have to use <code>@task.agent</code> or the operator classes. Pydantic AI works directly from a plain <code>@task</code>, <code>PythonOperator</code>, or any custom operator:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">pydantic_ai</span> <span class="kn">import</span> <span class="n">Agent</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.ai.hooks.pydantic_ai</span> <span class="kn">import</span> <span class="n">PydanticAIHook</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.ai.toolsets.sql</span> <span class="kn">import</span> <span class="n">SQLToolset</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.common.compat.sdk</span> <span class="kn">import</span> <span class="n">dag</span><span class="p">,</span> <span class="n">task</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="nd">@dag</span>
</span></span><span class="line"><span class="cl"><span class="k">def</span> <span class="nf">raw_pydantic_ai</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">    <span class="nd">@task</span>
</span></span><span class="line"><span class="cl">    <span class="k">def</span> <span class="nf">multi_agent</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">        <span class="n">hook</span> <span class="o">=</span> <span class="n">PydanticAIHook</span><span class="p">(</span><span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="n">model</span> <span class="o">=</span> <span class="n">hook</span><span class="o">.</span><span class="n">get_conn</span><span class="p">()</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">        <span class="n">agent</span> <span class="o">=</span> <span class="n">Agent</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">            <span class="n">model</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">            <span class="n">system_prompt</span><span class="o">=</span><span class="s2">"You are a SQL analyst."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">            <span class="n">toolsets</span><span class="o">=</span><span class="p">[</span><span class="n">SQLToolset</span><span class="p">(</span><span class="n">db_conn_id</span><span class="o">=</span><span class="s2">"postgres_default"</span><span class="p">)],</span>
</span></span><span class="line"><span class="cl">        <span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="n">result</span> <span class="o">=</span> <span class="n">agent</span><span class="o">.</span><span class="n">run_sync</span><span class="p">(</span><span class="s2">"What are the top products by revenue?"</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">return</span> <span class="n">result</span><span class="o">.</span><span class="n">output</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">    <span class="n">multi_agent</span><span class="p">()</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">raw_pydantic_ai</span><span class="p">()</span>
</span></span></code></pre></div><p>This gives you full control: run multiple agent calls in one task, swap models at runtime, combine outputs from different agents before returning.</p>
<p><code>@task.agent</code> adds guardrails on top (durable execution, HITL review, automatic tool logging). Raw Pydantic AI skips those. Both paths use the same toolsets.</p>
<h2 id="human-in-the-loop">Human-in-the-Loop</h2>
<p>Not every LLM output should go straight to production. The provider has two levels of human oversight.</p>
<p><strong>Approval gates</strong>: the task defers after generating output and waits for a human to approve before downstream tasks run:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="n">LLMOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">task_id</span><span class="o">=</span><span class="s2">"summarize_report"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">prompt</span><span class="o">=</span><span class="s2">"Summarize the quarterly financial report for stakeholders."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">require_approval</span><span class="o">=</span><span class="kc">True</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">approval_timeout</span><span class="o">=</span><span class="n">timedelta</span><span class="p">(</span><span class="n">hours</span><span class="o">=</span><span class="mi">24</span><span class="p">),</span>
</span></span><span class="line"><span class="cl">    <span class="n">allow_modifications</span><span class="o">=</span><span class="kc">True</span><span class="p">,</span>  <span class="c1"># reviewer can edit the output</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span>
</span></span></code></pre></div><p><strong>Iterative review</strong>: a human reviews agent output, approves, rejects, or requests changes, and the agent revises in a loop:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="n">AgentOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">task_id</span><span class="o">=</span><span class="s2">"analyst"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">prompt</span><span class="o">=</span><span class="s2">"Summarize the Q4 sales report."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">enable_hitl_review</span><span class="o">=</span><span class="kc">True</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">max_hitl_iterations</span><span class="o">=</span><span class="mi">5</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">hitl_timeout</span><span class="o">=</span><span class="n">timedelta</span><span class="p">(</span><span class="n">minutes</span><span class="o">=</span><span class="mi">30</span><span class="p">),</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span>
</span></span></code></pre></div><p>A built-in plugin adds the review UI to the Airflow web interface.</p>
<p><img alt="Human-in-the-loop approval interface showing the generated output with approve, reject, and modify options" src="/blog/common-ai-provider/images/hitl-approval.png" /></p>
<p><img alt="Human-in-the-loop review tab in the task instance page" src="/blog/common-ai-provider/images/hitl-review-tab.gif" /></p>
<h2 id="durable-execution">Durable Execution</h2>
<p>LLM agent calls are expensive. When a 10-step agent task fails on step 8, a retry shouldn&rsquo;t re-run all 10 steps and double your API bill. A single parameter fixes this:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-python"><span class="line"><span class="cl"><span class="n">AgentOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="n">task_id</span><span class="o">=</span><span class="s2">"analyst"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">prompt</span><span class="o">=</span><span class="s2">"Analyze quarterly trends across all regions."</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">llm_conn_id</span><span class="o">=</span><span class="s2">"my_openai_conn"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">toolsets</span><span class="o">=</span><span class="p">[</span><span class="n">SQLToolset</span><span class="p">(</span><span class="n">db_conn_id</span><span class="o">=</span><span class="s2">"postgres_default"</span><span class="p">)],</span>
</span></span><span class="line"><span class="cl">    <span class="n">durable</span><span class="o">=</span><span class="kc">True</span><span class="p">,</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span>
</span></span></code></pre></div><p>With <code>durable=True</code>, each model response and tool result is cached to <code>ObjectStorage</code> as the agent runs. On retry, cached steps replay instantly: no repeated LLM calls, no repeated tool execution. The cache is deleted after successful completion.</p>
<p>Say the agent ran <code>list_tables</code>, <code>get_schema</code>, <code>get_schema</code>, <code>query</code>, then hit a transient failure:</p>
<p><img alt="Attempt 1: agent runs tool calls then fails on a transient error" src="/blog/common-ai-provider/images/durable-attempt-1.png" /></p>
<p>On retry, those 4 tool calls and 4 model responses replay from cache in milliseconds. The agent picks up exactly where it left off:</p>
<p><img alt="Attempt 2: cached steps replayed instantly, agent continues from where it left off" src="/blog/common-ai-provider/images/durable-attempt-2.png" /></p>
<p>The summary line tells you exactly what happened:</p>
<p><img alt="Durable execution summary showing replayed vs fresh steps" src="/blog/common-ai-provider/images/durable-summary.png" /></p>
<p>Works with any <code>ObjectStorage</code> backend (local filesystem for dev, S3/GCS for production) and any toolset.</p>
<h2 id="any-model-one-interface">Any Model, One Interface</h2>
<p>Configure your LLM connection once. Switch providers by changing the connection, not the DAG code.</p>
<p>Four connection types:</p>
<table>
  <thead>
      <tr>
          <th>Connection Type</th>
          <th>Provider</th>
          <th>Model Format</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><code>pydanticai</code></td>
          <td>OpenAI, Anthropic, Groq, Mistral, Ollama, vLLM, and others</td>
          <td><code>openai:gpt-5</code>, <code>anthropic:claude-sonnet-4-20250514</code></td>
      </tr>
      <tr>
          <td><code>pydanticai-azure</code></td>
          <td>Azure OpenAI</td>
          <td><code>azure:gpt-4o</code></td>
      </tr>
      <tr>
          <td><code>pydanticai-bedrock</code></td>
          <td>AWS Bedrock</td>
          <td><code>bedrock:us.anthropic.claude-opus-4-5</code></td>
      </tr>
      <tr>
          <td><code>pydanticai-vertex</code></td>
          <td>Google Vertex AI</td>
          <td><code>google-vertex:gemini-2.0-flash</code></td>
      </tr>
  </tbody>
</table>
<p>Each type has dedicated UI fields in Airflow&rsquo;s connection form (API keys, endpoints, region, project, service account info), all stored in Airflow&rsquo;s secret backend.</p>
<p>Under the hood, the agent runtime is <a href="https://ai.pydantic.dev/">Pydantic AI</a>, which handles structured output, tool calling, and conversation management with proper typing.</p>
<h2 id="full-observability">Full Observability</h2>
<p>Every LLM task logs token usage and tool calls to Airflow&rsquo;s metadata DB. The full conversation history is stored too, so you can audit what the agent did after the fact.</p>
<p><code>AgentOperator</code> enables tool logging by default. Each tool call appears at INFO level with execution time, arguments at DEBUG level.</p>
<p><img alt="Tool call logging showing collapsible log groups with timing in the Airflow UI" src="/blog/common-ai-provider/images/tool-logging.png" /></p>
<h2 id="whats-next">What&rsquo;s Next</h2>
<p>These are directions we&rsquo;re exploring, not commitments. What actually ships depends on what the community needs. Tell us what matters to you.</p>
<ul>
<li><strong>Google ADK backend</strong>: <code>AgentOperator</code> with Google&rsquo;s Agent Development Kit as an alternative to Pydantic AI, with session management, ADK callbacks, and multi-agent workflows</li>
<li><strong>Asset integration</strong>: automatic schema context from Airflow Assets, lineage tracking for LLM-generated queries</li>
<li><strong>Cost controls (AIBudget)</strong>: token limits and cost caps per task, DAG, or team</li>
<li><strong>Multi-agent orchestration</strong>: patterns for composing agents across tasks</li>
<li><strong>Model evaluation</strong>: integration with pydantic-evals for testing LLM behavior</li>
</ul>
<h2 id="get-involved">Get Involved</h2>
<ul>
<li><a href="https://pypi.org/project/apache-airflow-providers-common-ai/">Install the provider</a> and try it out</li>
<li><a href="https://airflow.apache.org/registry/providers/common-ai/">Browse the provider on the Airflow Registry</a> for the full module listing</li>
<li><a href="https://airflow.apache.org/docs/apache-airflow-providers-common-ai/">Read the docs</a> for operator guides, toolset reference, and connection setup</li>
<li><strong>Give us feedback</strong>: open an <a href="https://github.com/apache/airflow/issues">issue</a>, start a thread on <a href="https://s.apache.org/airflow-slack">Airflow Slack</a>, or post to the <a href="https://airflow.apache.org/community/">dev mailing list</a>. What hooks should we add built-in toolsets for? What&rsquo;s missing?</li>
<li>The code lives in <a href="https://github.com/apache/airflow/tree/main/providers/common/ai"><code>providers/common/ai/</code></a> in the main Airflow repo</li>
</ul>

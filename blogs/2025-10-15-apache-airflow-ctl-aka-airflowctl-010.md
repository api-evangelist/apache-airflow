---
title: "Apache Airflow CTL aka airflowctl 0.1.0"
url: "https://airflow.apache.org/blog/airflowctl-0.1.0/"
date: "2025-10-15T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>We are thrilled to announce the first major release of <strong><code>airflowctl</code> 0.1.0</strong>, the new <strong>secure, API-driven command-line interface (CLI)</strong> for Apache Airflow — built under <a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-81&#43;Enhanced&#43;Security&#43;in&#43;CLI&#43;via&#43;Integration&#43;of&#43;API"><strong>AIP-81</strong></a>.</p>
<p>This release marks CLI to join the general posture on communicating through API. Airflow CLI joins the modern era of secure, auditable, and remote-first operations.</p>
<p><strong>Details</strong>:</p>
<p>📦 <strong>PyPI:</strong> <a href="https://pypi.org/project/apache-airflow-ctl/0.1.0/">https://pypi.org/project/apache-airflow-ctl/0.1.0/</a>  <br />
🛠️ <strong>Release Notes:</strong> <a href="https://airflow.apache.org/docs/apache-airflow-ctl/stable/release_notes.html">https://airflow.apache.org/docs/apache-airflow-ctl/stable/release_notes.html</a>  <br />
🪶 <strong>Source Code:</strong> <a href="https://github.com/apache/airflow/tree/main/airflow-ctl">https://github.com/apache/airflow/tree/main/airflow-ctl</a></p>
<h2 id="-what-is-airflowctl">🎯 What is airflowctl?</h2>
<p><code>airflowctl</code> is a new command-line interface for Apache Airflow that interacts exclusively with the Airflow REST API.
It provides a secure, auditable, and consistent way to manage Airflow deployments — without direct access to the metadata database.</p>
<h2 id="-coexistence-with-airflow-cli">🔄 Coexistence with Airflow CLI</h2>
<p>The Airflow CLI will continue as intended, primarily for admin tasks such as running Airflow components (<code>airflow api-server</code>, <code>airflow scheduler</code>) or managing the metadata database (<code>airflow db init</code>).
<code>airflowctl</code> focuses on operational commands that interact with Airflow resources via the API (<code>airflowctl dagrun trigger</code>, <code>airflowctl connection create</code>, etc.).</p>
<p>We defined the commands falls under <strong>two main categories</strong>:</p>
<ol>
<li><strong>Remote Commands</strong>: Operations that can be provided via API (e.g., managing DAGs, connections, variables, triggering DAG runs) are now available in <code>airflowctl</code> and will be the recommended approach going forward.</li>
<li><strong>Local/Admin Commands</strong>: Operations that manage Airflow components or the metadata database will remain in the Airflow CLI.</li>
</ol>
<p>Of course, in the current state they will both have the remote commands.
We are planning a zero-disruption migration path where <strong>Remote Commands</strong> will be gradually deprecated from the Airflow CLI as they achieve parity in <code>airflowctl</code>.</p>
<h2 id="-why-airflowctl">🔒 Why airflowctl?</h2>
<p>Until now, Airflow CLI connected directly to the <strong>metadata database</strong>, bypassing RBAC, authentication, and API logs.
While convenient, this approach limited <strong>security, auditing, and remote management</strong> capabilities — especially for enterprise environments.</p>
<p><strong><code>airflowctl</code></strong> changes that by routing every command through the <strong>Airflow REST API</strong>, bringing:</p>
<ul>
<li><strong>Authentication &amp; RBAC enforcement</strong></li>
<li><strong>Centralized logging &amp; audit trail</strong></li>
<li><strong>Secure credential storage via Keyring</strong></li>
<li><strong>Remote command execution with zero DB access</strong></li>
<li><strong>Consistency with Airflow UI and API behaviors</strong></li>
</ul>
<h2 id="-aip-81-cli-reimagined-through-the-api">🚀 AIP-81: CLI Reimagined Through the API</h2>
<p><strong>AIP-81</strong> (“Enhanced Security in CLI via Integration of API”) defined a clear goal:</p>
<blockquote>
<p>“The CLI must be a first-class, secure client of the Airflow REST API — not a privileged database actor.”</p></blockquote>
<p><code>airflowctl</code> is the direct realization of that vision.</p>
<h3 id="core-design-principles">Core design principles:</h3>
<ul>
<li><strong>All remote commands use the REST API</strong></li>
<li><strong>RBAC and auth handled consistently via API layer</strong></li>
<li><strong>Pluggable auth mechanisms</strong> (basic auth, OAuth, token, etc.)</li>
<li><strong>Secure credential persistence</strong> through <strong>system keyring</strong></li>
<li><strong>Extensible</strong> to new API endpoints as Airflow evolves</li>
</ul>
<h2 id="-getting-started">⚙️ Getting Started</h2>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-bash"><span class="line"><span class="cl">pip install apache-airflow-ctl
</span></span></code></pre></div><p>Once installed, you can connect your CLI to an Airflow instance:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-bash"><span class="line"><span class="cl">airflowctl auth login --url http://localhost:8080 --username admin --password admin
</span></span></code></pre></div><p>The password field is interactive by default. You can enter your password securely without echoing it on the terminal.
Use the above command without specifying the password and run it.</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-bash"><span class="line"><span class="cl">airflowctl auth login --url http://localhost:8080 --username admin --password
</span></span></code></pre></div><h2 id="-command-highlights">🧩 Command Highlights</h2>
<p>Here’s a quick look at some of the most popular commands, now fully API-backed in airflowctl 0.1.0:</p>
<h3 id="-assets">🧩 Assets</h3>
<p><img alt="Assets Create Event" src="/blog/airflowctl-0.1.0/images/assets_create_event.gif" />
<img alt="Assets Get" src="/blog/airflowctl-0.1.0/images/assets_get.gif" /></p>
<h3 id="-config">⚙️ Config</h3>
<p><img alt="Config Get" src="/blog/airflowctl-0.1.0/images/config_get.gif" /></p>
<h3 id="-connections">🔑 Connections</h3>
<p><img alt="Connections Create" src="/blog/airflowctl-0.1.0/images/connections_create.gif" />
<img alt="Connections Update" src="/blog/airflowctl-0.1.0/images/connections_update.gif" /></p>
<h3 id="-dag-runs">🎯 DAG Runs</h3>
<p>Trigger and inspect DAG runs securely through the API:</p>
<p><img alt="DagRun List" src="/blog/airflowctl-0.1.0/images/dagrun_list.gif" />
<img alt="DagRun Trigger" src="/blog/airflowctl-0.1.0/images/dagrun_trigger.gif" /></p>
<h3 id="-variables">🪣 Variables</h3>
<p><img alt="Variables Export" src="/blog/airflowctl-0.1.0/images/variables_export.gif" />
<img alt="Variables Import" src="/blog/airflowctl-0.1.0/images/variables_import.gif" /></p>
<p>All these commands — and many more — operate via Airflow’s public REST API, ensuring secure, logged, and RBAC-controlled execution.</p>
<h2 id="-key-security-features">🔐 Key Security Features</h2>
<h3 id="-keyring-integration">🔑 Keyring Integration</h3>
<p>No more plaintext tokens or passwords.
airflowctl uses your OS-level keyring (e.g., macOS Keychain, Windows Credential Manager, or Linux Secret Service) to store and retrieve authentication tokens securely.</p>
<h3 id="-role-based-access-control">🧱 Role-Based Access Control</h3>
<p>Every command is evaluated by Airflow’s RBAC system through the API — ensuring consistent authorization with the web UI and API clients.</p>
<h3 id="-auditing-and-traceability">🕵️‍♀️ Auditing and Traceability</h3>
<p>All CLI commands generate API logs and can be observed through standard audit mechanisms — closing a long-standing gap between the CLI and Airflow’s security model.</p>
<h2 id="-roadmap-highlights">📈 Roadmap Highlights</h2>
<p>airflowctl 0.1.0 is just the beginning. The foundation is in place for a fully unified, secure CLI experience.</p>
<h3 id="-coming-soon">🧩 Coming Soon</h3>
<ul>
<li>Completeness of API coverage</li>
<li>Live log streaming</li>
<li>Worker management</li>
<li>Remote debugging</li>
<li>Incremental deprecation of legacy CLI commands</li>
<li>Over time, the legacy airflow CLI will be incrementally deprecated as commands achieve API parity.</li>
</ul>
<h2 id="-migration">🧭 Migration</h2>
<p>Migration requires mapping commands, updating authentication, and re-testing automation to ensure compatibility with the new API-backed architecture.
Because airflowctl mirrors the core CLI syntax, most workflows require minimal changes — primarily adjusting authentication and configuration.</p>
<p>Side by side comparison:</p>
<table>
  <thead>
      <tr>
          <th>Before</th>
          <th>After</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><img alt="pools_list_old.gif" src="/blog/airflowctl-0.1.0/images/pools_list_old.gif" /></td>
          <td><img alt="pools_list.gif" src="/blog/airflowctl-0.1.0/images/pools_list.gif" /></td>
      </tr>
      <tr>
          <td><img alt="variables_list_old.gif" src="/blog/airflowctl-0.1.0/images/variables_list_old.gif" /></td>
          <td><img alt="variables_list_yaml.gif" src="/blog/airflowctl-0.1.0/images/variables_list_yaml.gif" /></td>
      </tr>
  </tbody>
</table>
<h2 id="-community--acknowledgments">🙏 Community &amp; Acknowledgments</h2>
<p>This release is the result of extensive collaboration across the Apache Airflow community.
Many thanks all who worked on AIP-81, the Airflow REST API, Authentication, and the airflowctl implementation.</p>
<h2 id="leading-contributors">Leading Contributors</h2>
<p>Special thanks to leading contributors of <code>airflowctl</code>:
<strong>Amar Prakash Pandey, Amogh Desai, Aritra Basu, Aryan Khurana, ayush3singh, Brent Bovenzi, Brunda10,
Bugra Ozturk, Daniel Standish, D. Ferruzzi, Deji Ibrahim, Elad Kalif, Ephraim Anierobi, GPK,
Guan Ming(Wesley) Chiu, Hussein Awala, Jake Roach, Jarek Potiuk, Jed Cunningham, Jens Scheffler,
Jaejun Lee, Kalyan R, Karthikeyan Singaravelan, Kaxil Naik, Kevin Yang, Kiruban Kamaraj, LI,JHE-CHEN,
Pierre Jeambrun, Pratiksha, Sam Wheating, Tzu-ping Chung, Valentyn, Vincent, Wei Lee, Yeonguk,
Yunchi Pang, Zhen-Lun (Kevin) Hong</strong></p>
<p>✨ In Summary</p>
<p>airflowctl 0.1.0 makes Airflow’s command line:</p>
<table>
  <thead>
      <tr>
          <th>Before</th>
          <th>After</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>Direct DB access</td>
          <td>API-backed security</td>
      </tr>
      <tr>
          <td>No RBAC or audit</td>
          <td>Centralized auth &amp; logging</td>
      </tr>
      <tr>
          <td>Inconsistent behavior</td>
          <td>Unified CLI + API experience</td>
      </tr>
      <tr>
          <td>Manual secrets</td>
          <td>Keyring-secured credentials</td>
      </tr>
  </tbody>
</table>
<p>Security first. API always. CLI reimagined.
The secure CLI foundation lays the groundwork for Airflow’s next generation. A unified, API-first platform for orchestration and operations.</p>

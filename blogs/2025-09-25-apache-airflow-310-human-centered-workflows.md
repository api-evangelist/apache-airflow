---
title: "Apache Airflow 3.1.0: Human-Centered Workflows"
url: "https://airflow.apache.org/blog/airflow-3.1.0/"
date: "2025-09-25T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>We are thrilled to announce the release of <strong>Apache Airflow 3.1.0</strong>, an update that puts humans at the center of data
workflows. This release introduces powerful new capabilities for human decision-making in automated
processes, comprehensive internationalization support, and significant developer experience improvements.</p>
<p><strong>Details</strong>:</p>
<p>📦 PyPI: <a href="https://pypi.org/project/apache-airflow/3.1.0/">https://pypi.org/project/apache-airflow/3.1.0/</a> <br />
📚 Core Airflow Docs: <a href="https://airflow.apache.org/docs/apache-airflow/3.1.0/">https://airflow.apache.org/docs/apache-airflow/3.1.0/</a> <br />
📚 Task SDK Docs: <a href="https://airflow.apache.org/docs/task-sdk/1.1.0/">https://airflow.apache.org/docs/task-sdk/1.1.0/</a> <br />
🛠️ Release Notes: <a href="https://airflow.apache.org/docs/apache-airflow/3.1.0/release_notes.html">https://airflow.apache.org/docs/apache-airflow/3.1.0/release_notes.html</a> <br />
🪶 Sources: <a href="https://airflow.apache.org/docs/apache-airflow/3.1.0/installation/installing-from-sources.html">https://airflow.apache.org/docs/apache-airflow/3.1.0/installation/installing-from-sources.html</a> <br />
🚏 Constraints: <a href="https://github.com/apache/airflow/tree/constraints-3.1.0">https://github.com/apache/airflow/tree/constraints-3.1.0</a></p>
<h2 id="-human-in-the-loop-hitl-when-automation-meets-human-judgment">🤝 Human-in-the-Loop (HITL): When Automation Meets Human Judgment</h2>
<p>This powerful capability bridges the gap between automated processes and human expertise, making Airflow invaluable for:</p>
<p><img alt="Human-in-the-Loop HITL" src="/blog/airflow-3.1.0/images/hitl.gif" /></p>
<ul>
<li><strong>AI/ML Model Validation</strong>: Pause inference pipelines for human review of model outputs</li>
<li><strong>Content Moderation</strong>: Route content through human reviewers before publication</li>
<li><strong>Approval Workflows</strong>: Require manager approval for sensitive operations</li>
<li><strong>Data Quality Gates</strong>: Allow data stewards to validate critical datasets</li>
</ul>
<p><strong>HITL</strong> tasks pause in a deferred state while presenting intuitive web forms in the Airflow UI. Users with appropriate roles can review context data, DAG parameters, and XCom values before making informed decisions.</p>
<h2 id="example-code">Example Code:</h2>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-py"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.sdk</span> <span class="kn">import</span> <span class="n">DAG</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.standard.operators.hitl</span> <span class="kn">import</span> <span class="n">HITLOperator</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">with</span> <span class="n">DAG</span><span class="p">(</span><span class="s2">"content_moderation"</span><span class="p">,</span> <span class="n">schedule</span><span class="o">=</span><span class="s2">"@daily"</span><span class="p">)</span> <span class="k">as</span> <span class="n">dag</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">    <span class="n">moderate_content</span> <span class="o">=</span> <span class="n">HITLOperator</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">task_id</span><span class="o">=</span><span class="s2">"review_content"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">message</span><span class="o">=</span><span class="s2">"Please review this content for publication"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">data_key</span><span class="o">=</span><span class="s2">"content_to_review"</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span></code></pre></div><h1 id="-ui-enhancements--performance">📊 UI Enhancements &amp; Performance</h1>
<h2 id="calendar-and-gantt-views-make-their-comeback">Calendar and Gantt Views Make Their Comeback</h2>
<p>Remember those beloved Calendar and Gantt chart views from Airflow 2.x? They&rsquo;re back, completely rebuilt for the
modern React UI after being omitted from the 3.0 release.</p>
<p>The new Calendar view is genuinely interactive with filtering capabilities that make it easy to drill down
into specific time periods and dag states.</p>
<p><img alt="Calendar View" src="/blog/airflow-3.1.0/images/calendar.gif" /></p>
<p>The Gantt chart is now integrated directly into the grid view and renders much faster than the old
version, giving you that timeline perspective without the performance headaches.</p>
<p><img alt="Gantt View" src="/blog/airflow-3.1.0/images/gantt.png" /></p>
<h2 id="theme-updates-that-actually-matter">Theme Updates That Actually Matter</h2>
<p>We&rsquo;ve refreshed the color palette using modern design principles, making the UI more consistent, professional
and most of all taken a careful look at contrast ratios so the UI should be more accessible.</p>
<h2 id="other-improvements">Other Improvements</h2>
<p>We&rsquo;ve added a lot more filtering options across the pages!</p>
<p>Plus, you can now pin your <strong>favorite DAGs</strong> to keep them at the top of your list or to filter for them easily. It&rsquo;s
one of those small features that makes a huge difference when dealing with 100s of workflows.</p>
<p><img alt="Favorite Dags in the UI" src="/blog/airflow-3.1.0/images/favorite.png" />
Credited to Volker Janz.</p>
<blockquote>
<p>📊 <strong>UI Development Milestone</strong>: Airflow 3.1.0 features <strong>5x more UI pull requests</strong> than the 2.10 release and <strong>50% more</strong> than Airflow 3.0, demonstrating the community&rsquo;s commitment to user experience excellence.</p></blockquote>
<h1 id="-deadline-alerts-proactive-workflow-monitoring">⏰ <strong>Deadline Alerts</strong>: Proactive Workflow Monitoring</h1>
<p>Say goodbye to reactive monitoring. <strong>Deadline Alerts</strong> provide proactive notifications when DAG runs
exceed time thresholds, helping ensure SLA compliance and timely completion of critical workflows.</p>
<p>Configure monitoring by specifying:</p>
<ul>
<li><strong>Reference point</strong>: DAG queued time, logical date, or fixed datetime</li>
<li><strong>Interval</strong>: Time threshold (positive or negative)</li>
<li><strong>Callback</strong>: Notifications via Airflow Notifiers or custom functions</li>
</ul>
<h2 id="example-code-1">Example Code:</h2>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-py"><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">datetime</span> <span class="kn">import</span> <span class="n">timedelta</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.sdk.definitions.deadline</span> <span class="kn">import</span> <span class="n">DeadlineAlert</span><span class="p">,</span> <span class="n">DeadlineReference</span><span class="p">,</span> <span class="n">AsyncCallback</span>
</span></span><span class="line"><span class="cl"><span class="kn">from</span> <span class="nn">airflow.providers.slack.notifications.slack_webhook</span> <span class="kn">import</span> <span class="n">SlackWebhookNotifier</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">with</span> <span class="n">DAG</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="s2">"critical_etl"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">deadline</span><span class="o">=</span><span class="n">DeadlineAlert</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="n">reference</span><span class="o">=</span><span class="n">DeadlineReference</span><span class="o">.</span><span class="n">DAGRUN_QUEUED_AT</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="n">interval</span><span class="o">=</span><span class="n">timedelta</span><span class="p">(</span><span class="n">hours</span><span class="o">=</span><span class="mi">2</span><span class="p">),</span>
</span></span><span class="line"><span class="cl">        <span class="n">callback</span><span class="o">=</span><span class="n">AsyncCallback</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">            <span class="n">SlackWebhookNotifier</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">            <span class="n">kwargs</span><span class="o">=</span><span class="p">{</span><span class="s2">"text"</span><span class="p">:</span> <span class="s2">"🚨 Critical ETL missed deadline!"</span><span class="p">}</span>
</span></span><span class="line"><span class="cl">        <span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="p">)</span> <span class="k">as</span> <span class="n">dag</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">    <span class="c1"># Your tasks here</span>
</span></span></code></pre></div><p>Perfect for monitoring daily ETLs, alerting before critical deadlines, or escalating resource-constrained workflows.</p>
<h1 id="-going-global-with-17-languages">🌍 Going Global with 17 Languages</h1>
<p>Airflow now speaks your team&rsquo;s language. Literally. We have added comprehensive internationalization support
for <strong>17 languages</strong>, including Arabic, Chinese, French, German, Spanish and more. The interface detects your
browser preferences automatically, but you can switch languages on the fly without refreshing the page.</p>
<p><img alt="Internationalization Demo" src="/blog/airflow-3.1.0/images/i18n-demo.gif" /></p>
<p>For our Arabic and Hebrew users, we&rsquo;ve built in <strong>proper right-to-left (RTL) support</strong></p>
<p>The best part? We have made it straightforward for the community to contribute additional languages with clear
contribution guidelines, so this is just the beginning of Airflow&rsquo;s global reach.</p>
<h1 id="-build-your-airflow-your-way">🎨 Build <em>your Airflow</em>, <em>your way</em></h1>
<p>The new <strong>React Plugin System</strong> (<strong>AIP-68</strong>) transforms how you extend Airflow&rsquo;s interface. We have replaced
the old Flask-based approach with a modern toolkit that lets you customize Airflow exactly how your team works.</p>
<p>Want to embed your company&rsquo;s dashboard right in the Airflow UI? Build React applications or iframes that will
render inside Airflow&rsquo;s (nav bar, dashboard, details page, etc.). Want to link to your existing tools
seamlessly? Create custom external links to your resources. Want to extend Airflow&rsquo;s API server? Register
FastAPI sub applications and middlewares that fit your specific processes.</p>
<p>The system includes:</p>
<ul>
<li><strong>External Views</strong> for linking to existing tools (external links or embedded iframes)</li>
<li><strong>React Applications</strong>  support for rendering external react apps</li>
<li><strong>FastAPI Sub Applications</strong> to extend the API server</li>
<li><strong>Root Middlewares</strong> for intercepting API requests (even core ones)</li>
</ul>
<p>We&rsquo;ve already seen teams integrate everything from Wikipedia searches to data lineage
visualizations to yes, someone building a snake game to play while waiting on dag runs!</p>
<p><img alt="Snake Game Plugin" src="/blog/airflow-3.1.0/images/snake.gif" />
Credited to Tamara Fingerlin.</p>
<h1 id="-enhanced-developer-and-authoring-experience">🔧 Enhanced Developer and Authoring Experience</h1>
<h2 id="task-sdk-evolution">Task SDK Evolution</h2>
<p>Airflow 3.1 advances the decoupling of the <strong>Task SDK</strong> from Airflow Core through improved DAG serialization. While
complete separation arrives in 3.2.0, the foundation enables:</p>
<ul>
<li><strong>Independent Upgrades</strong>: Reduced coordination need between Dag authors and Airflow Ops teams</li>
<li><strong>Forward Compatibility</strong>: Dag authors should now write Dags by importing from the <strong>airflow.sdk</strong> namespace for future-proofing. (Naturally, the old imports still work but issue a warning.)</li>
<li><strong>Deployment Flexibility</strong>: Better support for separated component deployment</li>
</ul>
<h2 id="python-313-support">Python 3.13 Support</h2>
<p>Airflow 3.1.0 adds <strong>Python 3.13</strong> support while removing Python 3.9 (end-of-life). The platform now supports Python 3.10, 3.11, 3.12, and 3.13.</p>
<h2 id="inference-execution">Inference Execution</h2>
<p>A new streaming API endpoint (<strong><code>/dags/{dag_id}/dagRuns/{dag_run_id}/wait</code></strong>) allows applications to watch DAG runs
until completion, enabling responsive integration patterns for real-time workflows.</p>
<p>The below example use <a href="https://www.python-httpx.org/async/"><code>httpx</code></a> to trigger a dag run, and emits the final dag run
state after it finishes:</p>
<div class="highlight"><pre class="chroma" tabindex="0"><code class="language-py"><span class="line"><span class="cl"><span class="kn">import</span> <span class="nn">asyncio</span>
</span></span><span class="line"><span class="cl"><span class="kn">import</span> <span class="nn">json</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="kn">import</span> <span class="nn">httpx</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">dag_id</span> <span class="o">=</span> <span class="s2">"my-dag"</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">async</span> <span class="k">def</span> <span class="nf">create_and_wait</span><span class="p">(</span><span class="n">client</span><span class="p">):</span>
</span></span><span class="line"><span class="cl">    <span class="c1"># Create a dag run...</span>
</span></span><span class="line"><span class="cl">    <span class="n">r</span> <span class="o">=</span> <span class="k">await</span> <span class="n">client</span><span class="o">.</span><span class="n">post</span><span class="p">(</span><span class="sa">f</span><span class="s2">"https://my-airflow.example.com/api/v2/dags/</span><span class="si">{</span><span class="n">dag_id</span><span class="si">}</span><span class="s2">/dagRuns"</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="n">run_id</span> <span class="o">=</span> <span class="n">r</span><span class="o">.</span><span class="n">json</span><span class="p">()[</span><span class="s2">"dag_run_id"</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">    <span class="k">async</span> <span class="k">with</span> <span class="n">client</span><span class="o">.</span><span class="n">stream</span><span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="s2">"GET"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">        <span class="sa">f</span><span class="s2">"https://my-airflow.example.com/api/v2/dags/</span><span class="si">{</span><span class="n">dag_id</span><span class="si">}</span><span class="s2">/dagRuns/</span><span class="si">{</span><span class="n">run_id</span><span class="si">}</span><span class="s2">/wait"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">)</span> <span class="k">as</span> <span class="n">r</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">        <span class="k">async</span> <span class="k">for</span> <span class="n">line</span> <span class="ow">in</span> <span class="n">r</span><span class="o">.</span><span class="n">aiter_lines</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">            <span class="k">pass</span>  <span class="c1"># You can do progress report here instead.</span>
</span></span><span class="line"><span class="cl">    <span class="nb">print</span><span class="p">(</span><span class="s2">"Dag run state:"</span><span class="p">,</span> <span class="n">json</span><span class="o">.</span><span class="n">loads</span><span class="p">(</span><span class="n">line</span><span class="o">.</span><span class="n">strip</span><span class="p">())[</span><span class="s2">"state"</span><span class="p">])</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">async</span> <span class="k">def</span> <span class="nf">main</span><span class="p">():</span>
</span></span><span class="line"><span class="cl">    <span class="k">async</span> <span class="k">with</span> <span class="n">httpx</span><span class="o">.</span><span class="n">AsyncClient</span><span class="p">()</span> <span class="k">as</span> <span class="n">client</span><span class="p">:</span>
</span></span><span class="line"><span class="cl">        <span class="k">await</span> <span class="n">create_and_wait</span><span class="p">(</span><span class="n">client</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="n">asyncio</span><span class="o">.</span><span class="n">run</span><span class="p">(</span><span class="n">main</span><span class="p">())</span>
</span></span></code></pre></div><h1 id="-amazing-community">🙏 Amazing Community</h1>
<p>Apache Airflow 3.1.0 represents an extraordinary community effort, showcasing the vibrant ecosystem that drives this project forward with <strong>163 contributors</strong> making this release possible across <strong>1,400+ commits</strong>.</p>
<h2 id="leading-contributors">Leading Contributors</h2>
<p>Special thanks to our top 20 contributors who drove this release forward: <strong>Amogh Desai</strong>, <strong>Ash Berlin-Taylor</strong>, <strong>Brent Bovenzi</strong>, <strong>Bugra Ozturk</strong>, <strong>Daniel Standish</strong>, <strong>Elad Kalif</strong>, <strong>Ephraim Anierobi</strong>, <strong>GPK</strong>, <strong>Guan-Ming (Wesley) Chiu</strong>, <strong>Jarek Potiuk</strong>, <strong>Jens Scheffler</strong>, <strong>Karthikeyan Singaravelan</strong>, <strong>Kaxil Naik</strong>, <strong>LI,JHE-CHEN</strong>, <strong>Pierre Jeambrun</strong>, <strong>Shahar Epstein</strong>, <strong>Tzu-ping Chung</strong>, <strong>Vincent</strong>, <strong>Wei Lee</strong>, and <strong>Yeonguk Choo</strong>.</p>
<details>
View all 143 additional contributors
<p>1in3x, Aaron Chen, Aayush Bisen, Abhishek, Achim Gädke, Aldo, Alex Neal Albinda, Alyssa Mhie M. Matila, Anand Raman, Andrei Serdiukov, Ankit Chaurasia, Antony Southworth, Aritra Basu, Aryan Khurana, Atul Singh, Azis, BBQing, Bjorn Olsen, Bowrna, Brunda10, Carl Leake, Chang-Yen (Brian) Li, Christos Bisias, Collin McNulty, Constance Martineau, D. Ferruzzi, DHARMENDRA AHIRWAR, Damian Shaw, Daniel Wolf, David Blain, Denis Krivenko, Dev-iL, Dheeraj Turaga, Diogo Rodrigues, Domadelfin, Dov Benyomin Sohacheski, Duc Nguyen, Evgenii Prusov, Farhan, Fortytwo, Gabriel TOUZALIN, Gajo Petrovic, Gary Hsu, Glenn Schuurman, Guangyang Li, Gwak Beomgyu, Hoyeop Lee, Hussein Awala, Isaiah Iruoha, Ivan, Jake Roach, James Hyphen, Jason, Jason Brownstein, Jed Cunningham, Jeongseok Kang, John Bampton, Josef Šimánek, Josué Velázquez Gen, João Ramiro, Kacper Muda, Kalyan R, Karan Anand, Karen Braganza, Karthik S, Kavya Katal, Ken Lewerentz, Kevin Liu, Kevin Yang, Kiran R, Kiruban Kamaraj, Kosteev Eugene, Kumbha Lakshmi Narayana, Kyungjun Lee, LIU ZHE YOU, Lipu Fei, Maciej Obuchowski, Maksim, Mike Lay, Mikhail Dengin, Minkyu Kim, N R Navaneet, NOEUN KIM, Naseem Shah, Nataneljpwd, Niko Oliveira, Nithin U, Nitochkin, Olivier, Owen Leung, Paolo Facchinetti, Pedro Leal, Pratiksha, Przemysław Mirowski, Qiang-Liu, Rahul Vats, Ramit Kataria, Sam Wheating, Sean Ghaeli, Sean Rose, Sebastián Ortega, Seongho Kim, SeungMin, Shlomit-B, Shubham Raj, Sneha Prabhu, Stanley Law, Stephan, Steve Ahn, Valentyn, Vic Wen, Vincent Kling, VladaZakharova, Wei-Yu Chen, Wonseok Yang, Xch1, Xiaodong DENG, Y. SOMDA, Yann Lambret, Yannick Suter, Yeonguk, Yiming Peng, Yusin, Zach, Zach Liu, Zhen-Lun (Kevin) Hong, anasatzemoso, ayush3singh, codecae, davidfgcorreia, dominikhei, ecodina, fuatcakici, humit, magic_frog, majorosdonat, mandeepzemo, oboki, olegkachur-e, pawelgrochowicz, roach231428, shreyaskj-0710, sujitha-saranam, suman-himanshu, vikrantkumar-max, yangyulely, 코딩하는펭귄.</p>
</details>
<h2 id="ui-excellence--community-growth">UI Excellence &amp; Community Growth</h2>
<p>The exceptional growth in UI contributions - <strong>5x more pull requests</strong> than Airflow 2.10 and <strong>50% more</strong> than Airflow 3.0 - reflects the dedicated efforts of our UI maintainers and an expanding community of <strong>70 frontend contributors</strong> who have made user experience a cornerstone of this release.</p>
<h2 id="global-collaboration">Global Collaboration</h2>
<p>The internationalization effort represents contributors from around the world, making Airflow truly accessible across <strong>17 languages</strong> and diverse technical communities, demonstrating the truly global nature of the Airflow project.</p>
<hr />
<p><em>Apache Airflow is a community-driven project. Special thanks to all contributors who made this release possible through code, documentation, testing, and feedback. The future of workflow orchestration is built together.</em></p>
<h1 id="-migration--upgrade-notes">📝 Migration &amp; Upgrade Notes</h1>
<ul>
<li><strong>Python Support</strong>: Ensure you&rsquo;re running Python 3.10+ before upgrading. We recommend at least Python 3.12 for performance improvements from the Python core team – 3.13 if you can manage it is even better!</li>
<li><strong>Provider Updates</strong>: Update to the latest provider packages to take advantage of new features.</li>
<li><strong>Breaking Changes</strong>: Review the <a href="https://airflow.apache.org/docs/apache-airflow/3.1.0/installation/upgrading.html">migration guide</a> for configuration changes and removed features if you are upgrading directly from Airflow 2.x.</li>
</ul>
<h1 id="-get-involved">🔗 Get Involved</h1>
<ul>
<li><strong>Try the Release</strong>: Upgrade your development environment and explore the new features</li>
<li><strong>Join the Conversation</strong>: Connect with us on (<a href="https://s.apache.org/airflow-slack">Airflow Slack</a>) and the (<a href="https://airflow.apache.org/community/">dev mailing list</a>)</li>
<li><strong>Contribute</strong>: Check out our <a href="https://github.com/apache/airflow/blob/main/contributing-docs/README.rst">contribution guide</a>.</li>
<li><strong>Provide Feedback</strong>: Share your experiences and suggestions on GitHub (<a href="https://github.com/apache/airflow">https://github.com/apache/airflow</a>)</li>
</ul>
<p>Apache Airflow 3.1.0 marks a new chapter in making data orchestration more inclusive, intelligent, and
human-centered. We can&rsquo;t wait to see what you build with it!</p>

---
title: "Apache Airflow® 3 is Generally Available!"
url: "https://airflow.apache.org/blog/airflow-three-point-oh-is-here/"
date: "2025-04-22T00:00:00Z"
author: "Apache Airflow"
feed_url: "https://airflow.apache.org/blog/index.xml"
---
<p>We announced our intent to focus on Apache Airflow 3.0® as the next big milestone for the Airflow project at the Airflow Summit in September 2024. We are delighted to announce that Airflow 3.0 is now released!</p>
<h2 id="a-major-release-four-years-in-the-making">A Major Release, Four Years in the Making</h2>
<p>Airflow 3.0 is the biggest release in Airflow’s history—2.0 was released in 2020, and the last 4 years have seen incremental updates and releases every quarter with version 2.10 released in Q4 2024. With over 30 million monthly downloads (up over 30x since 2020) and 80,000 organizations (up from 25,000 in 2020) now using Airflow, we’ve seen an incredible growth in popularity since 2.0.</p>
<p>Over the last four years, Airflow has grown to power business critical data workflows within organizations of all sizes. We have seen an exponential increase in the use cases for Airflow from its beginnings with ETL, ELT, and Reverse ETL, with over 30% of Airflow users using it for MLOps, and 10% using it for GenAI workflows. Airflow 3 is a response to this use case expansion and is the standard for data application development across the enterprise.</p>
<p>Here are some highlights:</p>
<ul>
<li>
<p>Airflow 3 is significantly easier to use for data practitioners and incorporates their key requests for critical changes to Airflow. Early user reactions to features such as the new React based UI, DAG Versioning, and improved Backfill support have been incredibly positive. I was ecstatic to see the reaction from data engineers when I demonstrated this at a recent Airflow meetup.</p>
</li>
<li>
<p>The seamless UI transition of navigating between Asset-oriented workflows and Task-oriented workflows is beautiful. Once again, Airflow lets the developer choose how you want to develop and navigate without imposing any restrictions.</p>
</li>
<li>
<p>Introduction of Event Driven Scheduling enables Airflow to seamlessly integrate with messaging providers and react to events happening and data assets being updated outside of Airflow.</p>
</li>
<li>
<p>The big architecture change with the introduction of the Task Execution Interface and the Task SDKs, enable a stronger security model, including secure, scalable execution across multi-cloud, hybrid-cloud, and local data center deployments.</p>
</li>
</ul>
<p>This is the result of 300+ developers within the Airflow community working together tirelessly for many months and I could not be more proud to be part of this wonderful team. Here are some more details of the release.</p>
<h2 id="highly-requested-ux-features">Highly requested UX features</h2>
<h3 id="dag-versioning">DAG Versioning</h3>
<p>DAG Versioning has been the most requested feature within Airflow based on the annual Airflow survey. As implemented in Airflow 3, a DAG will run through to completion based on the version at start, even if a new version has been uploaded while this DAG was being run. All DAG runs in the UI are now associated with the version of the DAG as run including the Task structure, the code, the logs, and more.
This is described in two AIPs: Improve DAG history (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-65%3A&#43;Improve&#43;DAG&#43;history&#43;in&#43;UI">AIP-65</a>) , and DAG Bundles and Parsing (<a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=294816356">AIP-66</a>).</p>
<p><img alt="DAG Versioning UI" src="/blog/airflow-three-point-oh-is-here/versioning_ui.gif" /></p>
<h3 id="backfills-improvements">Backfills improvements</h3>
<p>Another long-standing user request has been better support for backfills. Often discussed in the context of machine learning, backfills also apply to traditional ETL and ELT use cases.  In Airflow 3, backfills are run within the scheduler for improved control, scalability, and diagnostics. Backfills can now be started from the UI or API, and monitored within the UI.</p>
<p>This was defined as part of “Scheduler-managed backfills” (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-78&#43;Scheduler-managed&#43;backfill">AIP-78</a>), and an example screenshot is shown below:</p>
<p><img alt="Backfill UI" src="/blog/airflow-three-point-oh-is-here/backfill.png" /></p>
<h2 id="run-anywhere-at-any-time-in-any-language">Run anywhere, at any time, in any Language</h2>
<h3 id="run-anywhere-in-any-language">Run anywhere, in any language</h3>
<p>A foundational goal of Airflow 3 is allowing execution in any environment, in any language. A key component of this is the Task Execution Interface (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-72&#43;Task&#43;Execution&#43;Interface&#43;aka&#43;Task&#43;SDK">AIP-72</a>), which enables the evolution of Airflow into a client-server architecture, which represents one of the most significant architectural shifts in Airflow’s history. This supports Celery, Kubernetes, and Local Executors, but also enables new capabilities. A component of this change is the API server which represents input for the Task Execution Interface. This foundational feature enables multi-cloud deployments and multi-language support in the form of the Task Execution API. The Airflow 3 release includes the Python TaskSDK which enables backward compatibility for existing DAGs. TaskSDKs for additional languages, starting with Golang will be released over the next few months.</p>
<p>To enable data pipelines to be run on edge devices, outside of the core data centers and clouds, the Edge Executor (<a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=301795932">AIP-69</a>) is available as a provider package with Airflow 3. This is an incremental feature built on top of the Task Execution Interface. Initial incarnations have been released in experimental mode based on Airflow 2x and this executor has now evolved to leverage the Airflow 3 API Server.</p>
<h3 id="event-driven-scheduling-and-data-assets">Event-driven scheduling and Data Assets</h3>
<p>Airflow 3 represents a foundational jump in enabling Airflow to react to events happening outside of Airflow, including data assets being created or updated by external data systems. This was based on the evolution of Datasets into Data Assets and was broken out into several AIPs as detailed below, which are all part of the release.</p>
<p>The fundamental evolution of Datasets into Data Assets has been done as part of “Introducing Data Assets” (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-74&#43;Introducing&#43;Data&#43;Assets">AIP-74</a>). This introduces the concept of Watchers which is leveraged by other capabilities detailed below. A significant enhancement around Data Assets is the New Asset-Centric Syntax (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-75&#43;New&#43;Asset-Centric&#43;Syntax">AIP-75</a>) for defining Assets easily with DAGs using the Python decorator syntax, which is part of this release.</p>
<p>External event driven scheduling (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-82&#43;External&#43;event&#43;driven&#43;scheduling&#43;in&#43;Airflow">AIP-82</a>) is based on the foundational Data Assets work described above, specifically Watchers. The initial scope as defined in the AIP is complete and now incorporates a “Common Message Bus” interface. This release also includes an implementation of the above for AWS SQS as an “out of the box” integration, which demonstrates DAGs being triggered upon the arrival of a message in AWS SQS.</p>
<h3 id="inference-execution-and-hyperparameter-tuning">Inference execution and hyperparameter tuning</h3>
<p>Many ML and AI Engineers are already using Airflow for ML/AI Ops, especially for model training. However, there were challenges for Inference Execution. Enhancing Airflow for Inference Execution by adding support for non-data-interval-Dags (sorry, that’s a mouthful) is an important change. This work is covered as part of “Remove Execution date unique constraint from DAG run” (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-83&#43;Remove&#43;Execution&#43;Date&#43;Unique&#43;Constraint&#43;from&#43;DAG&#43;Run">AIP-83</a>)</p>
<h2 id="security-and-usability-improvements">Security and usability improvements</h2>
<h3 id="ui-modernization">UI Modernization</h3>
<p>The Airflow UI has been completely rewritten as part of Airflow 3 and incorporates a significantly improved user experience which seamlessly blends Asset-oriented workflows with Task-oriented workflows. This is a dramatic improvement which enables developers to author DAGs as they choose, without being opinionated about “a right way”.</p>
<p><img alt="Airflow 3.0’s new UI" src="/blog/airflow-three-point-oh-is-here/airflow-3.0-ui.gif" /></p>
<p>Check out <a href="http://airflow.apache.org/docs/apache-airflow/stable/ui.html">the screenshots in the docs</a> for more.</p>
<p>Recreating it to be based on React and the FastAPI has been a massive project and was broken out into several AIPs as detailed below.</p>
<p>The foundation for the new UI is the REST API and a set of internal APIs for UI Operations (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-84&#43;UI&#43;REST&#43;API">AIP-84</a>) both of which are now based on FastAPI. These APIs are served as part of the API Server described above as part of the Task Execution framework.</p>
<p>The Airflow 3.0 UI has been significantly improved and includes a streamlined user experience workflow encompassing both the Grid and Graph views. The interaction between DAGs and Assets are also more streamlined. User experience is always a work in progress and we very much appreciate your feedback. This is covered in great detail as part of the Modern Web Application proposal (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-38&#43;Modern&#43;Web&#43;Application">AIP-38</a>).</p>
<p>As part of this project, Flask AppBuilder has now been moved into a separate provider package and is no longer a part of the Core Airflow package. This enables an easier security and maintenance update process, while retaining backwards compatibility. This is documented as part of the “Remove Flask App Builder as a Core Dependency” proposal (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-79%3A&#43;Remove&#43;Flask&#43;AppBuilder&#43;as&#43;Core&#43;dependency">AIP-79</a>).</p>
<h3 id="security">Security</h3>
<p>A key benefit of the Task Execution Interface and the API server is Task Isolation. This has often been requested by Airflow enterprise deployments for a better security posture when an Airflow deployment is shared by multiple teams. Further security and authorization patterns can be developed on top of this foundation as more detailed requirements are uncovered.</p>
<p>Improving the CLI and reducing the maintenance burden by having the CLI use the Airflow APIs, rather than direct access is an important evolution for Airflow. We have now split the core Airflow CLI into two parts, the first for local development and backwards compatibility and the second for remote access using the API. The second will be a new provider package called “airflowctl” which can be optionally installed along with Core Airflow. This is documented in more detail as part of the “Enhanced security in CLI via Integration of API” proposal (<a href="https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-81&#43;Enhanced&#43;Security&#43;in&#43;CLI&#43;via&#43;Integration&#43;of&#43;API">AIP-81</a>).</p>
<h2 id="an-amazing-community">An amazing community</h2>
<p>This release could not have happened without the inspiration and technical leadership of key contributors who led the AIPs listed above. We thank them all here: Ash Berlin-Taylor, Brent Bovenzi, Bugra Ozturk, Constance Martineau, Daniel Standish, Jed Cunningham, Jens Scheffler, Kaxil Naik, Pierre Jeambrun, Vincent Beck, and Vikram Koka. We also wanted to thank Jarek Potiuk for the critical development infrastructure and packaging work and to Elad Kalif for shepherding all the key provider changes needed. We would like to recognize Wei Lee and Ankit Chaurasia for their work on the upgrade utilities to enable users to easily upgrade to Airflow 3.</p>
<p>Finally, a huge shoutout to Jed Cunningham and Kaxil Naik for the critical part of release management!</p>
<p>Over three hundred developers around the world have contributed to making this release a reality. We thank them all for their contributions. They are listed here in alphabetical order:</p>
<ul>
<li>Aakcht</li>
<li>Aaron Chen</li>
<li>Abhishek</li>
<li>Adam Turner</li>
<li>Adan</li>
<li>Aditya Yadav</li>
<li>Adrian Lazar</li>
<li>Adrian Perea</li>
<li>Ajit J Gupta</li>
<li>Albert Okiri</li>
<li>Alex Waygood</li>
<li>Alexander Millin</li>
<li>AlteredOracle</li>
<li>Amar Prakash Pandey</li>
<li>Amir Mor</li>
<li>Amogh Desai</li>
<li>Amol Saini</li>
<li>Anakin Skywalker Pactores</li>
<li>Andor Markus</li>
<li>Andre Miranda</li>
<li>Andres Lowrie</li>
<li>Andrew Arochukwu</li>
<li>Andrew Stein</li>
<li>Andrii Abramov</li>
<li>Andrii Korotkov</li>
<li>Andrii Yerko</li>
<li>Ankit Chaurasia</li>
<li>Anthony Lin</li>
<li>Antony Southworth</li>
<li>Aritra Basu</li>
<li>Arjun Pathak</li>
<li>Arnel Jan Sarmiento</li>
<li>Arnout Engelen</li>
<li>Artem Suslov</li>
<li>Arthur Braveheart</li>
<li>Artour</li>
<li>Artur Skarżyński</li>
<li>Arunav Gupta</li>
<li>Aryan Khurana</li>
<li>Ash Berlin-Taylor</li>
<li>AshKatzEm</li>
<li>AutomationDev85</li>
<li>Avihais12344</li>
<li>Azhar Izzannada E</li>
<li>Baitur Ulukbekov</li>
<li>Balthazar Rouberol</li>
<li>Bartosz Jankiewicz</li>
<li>Bas</li>
<li>Ben Chen</li>
<li>Benoit Perigaud</li>
<li>Biswamitra Biswas</li>
<li>Bjorn Olsen</li>
<li>Bluefox9x5</li>
<li>Bohdan Udovenko</li>
<li>Bonnie Why</li>
<li>Boris Morel</li>
<li>Bowrna</li>
<li>Brent Bovenzi</li>
<li>Bugra Ozturk</li>
<li>Błażej Tecław</li>
<li>Castle Cheng</li>
<li>Chris Luedtke</li>
<li>Christian Yarros</li>
<li>Christos Bisias</li>
<li>Collin McNulty</li>
<li>Computer Network Investigation</li>
<li>Constance Martineau</li>
<li>D. Ferruzzi</li>
<li>DShi</li>
<li>Daniel Gellert</li>
<li>Daniel Imberman</li>
<li>Daniel Standish</li>
<li>Daniel van der Ende</li>
<li>Danish Amjad</li>
<li>Danny Liu</li>
<li>David Blain</li>
<li>Derek</li>
<li>Detlev V.</li>
<li>Dewen Kong</li>
<li>Sriraj Dheeraj Turaga</li>
<li>Diogo Rodrigues</li>
<li>Dmitry Astankov</li>
<li>Dmitry Pustoshilov</li>
<li>Dominic Leung</li>
<li>Dong-yeong0</li>
<li>Doug Guthrie</li>
<li>Dylan Melotik</li>
<li>Elad Kalif</li>
<li>Eldar Kasmamytov</li>
<li>Ephraim Anierobi</li>
<li>Eric</li>
<li>Everton Seiei Arakaki</li>
<li>Farhan</li>
<li>Fedor Kobak</li>
<li>Felix Uellendall</li>
<li>Fred Thomsen</li>
<li>Fully.is(풀리)</li>
<li>GPK</li>
<li>Gagan Bhullar</li>
<li>Geonwoo Kim</li>
<li>GlenboLake</li>
<li>Gopal Dirisala</li>
<li>Gregory Borodin</li>
<li>Guan-Ming (Wesley) Chiu</li>
<li>Guangyang Li</li>
<li>Guillaume Lostis</li>
<li>Hari Selvarajan</li>
<li>HassanAlahmed</li>
<li>Hojin Jun</li>
<li>Howard Yoo</li>
<li>Huanjie Guo</li>
<li>Hung</li>
<li>Hussein Awala</li>
<li>Hyunsoo Kang</li>
<li>Ian Buss</li>
<li>Idris Adebisi</li>
<li>Igor Kholopov</li>
<li>IlaiGigi</li>
<li>Indrale Dnyaneshwar</li>
<li>JISHAN GARGACHARYA</li>
<li>Jaejun</li>
<li>Jake Ferriero</li>
<li>Jake Roach</li>
<li>Jakub Dardzinski</li>
<li>James Chaldecott</li>
<li>James Regan</li>
<li>Jarek Potiuk</li>
<li>Jasmin Patel</li>
<li>Jason</li>
<li>Jed Cunningham</li>
<li>Jeff Harrison</li>
<li>Jens Scheffler</li>
<li>Jianzhun Du</li>
<li>Jimmy McBroom</li>
<li>Joao Amaral</li>
<li>João Pedro M Miguel</li>
<li>Joel Labes</li>
<li>Joey Cumines</li>
<li>Joffrey Bienvenu</li>
<li>John Bampton</li>
<li>John C. Merfeld</li>
<li>Johnny1cyber</li>
<li>José Joaquín Virtudes Castro</li>
<li>Joseph Ang</li>
<li>JoshuaXOng</li>
<li>Josix</li>
<li>Julian Maicher</li>
<li>Kacper Kulczak</li>
<li>Kacper Muda</li>
<li>Kalyan R</li>
<li>Kamil Breguła</li>
<li>Karen Braganza</li>
<li>Karthik Dulam</li>
<li>Karthik Ravi</li>
<li>Karthikeyan Singaravelan</li>
<li>Kaxil Naik</li>
<li>Kevin Allen</li>
<li>Kim</li>
<li>Kris</li>
<li>Kunal Bhattacharya</li>
<li>LIU ZHE YOU</li>
<li>Lennox Stevenson</li>
<li>Linh</li>
<li>Lorin Dawson</li>
<li>Lou ✨</li>
<li>Lucy Hu</li>
<li>Lukas Mikelionis</li>
<li>Luyang Liu</li>
<li>Lyndon Fan</li>
<li>M. Olcay Tercanlı</li>
<li>Maciej Obuchowski</li>
<li>Madison Swain-Bowden</li>
<li>Maksim</li>
<li>Marcelo Trylesinski</li>
<li>Marcos Marx</li>
<li>Maria</li>
<li>Mark Andreev</li>
<li>Mark H</li>
<li>Matt Burke</li>
<li>Matt Dupree</li>
<li>Maxim Martynov</li>
<li>Mayuresh Kedari</li>
<li>Mehul Goyal</li>
<li>Mike</li>
<li>Mike Beckhusen</li>
<li>Mikhail Dengin</li>
<li>MishchenkoYuriy</li>
<li>Muhammad Hanif Mohamad Musa</li>
<li>Myles Hollowed</li>
<li>Narendra-Neerukonda</li>
<li>Natsu</li>
<li>Nikita</li>
<li>Niko Oliveira</li>
<li>Nishant Gupta</li>
<li>Nitesh Kumar Dubey Samsung</li>
<li>Nitochkin</li>
<li>Oleg Ovcharuk</li>
<li>Oleksandr Slynko</li>
<li>Omkar P</li>
<li>Owen Leung</li>
<li>Pandycool</li>
<li>Pankaj Koti</li>
<li>Park Jiwon</li>
<li>Pavan Sharma</li>
<li>Peng-Jui Wang</li>
<li>Peter Debelak</li>
<li>Phani Kumar</li>
<li>Pierre Jeambrun</li>
<li>Po-Yu Hsieh</li>
<li>Prajwal7842</li>
<li>Pratiksha</li>
<li>Purna Chander</li>
<li>Rafa</li>
<li>Rahul Madan</li>
<li>Rahul Vats</li>
<li>Ramit Kataria</li>
<li>Rishabh Srivastava</li>
<li>Rushabh Garambha</li>
<li>Ryan Eakman</li>
<li>Ryan Hatter</li>
<li>Rytis Ulys</li>
<li>SAI GANESH S</li>
<li>Sam Lendle</li>
<li>SamLiaoP</li>
<li>Saumil Patel</li>
<li>SaurabhhB</li>
<li>Sean Gabriel Bayron</li>
<li>Sean Rose</li>
<li>Sebastian Daum</li>
<li>SeonghwanLee</li>
<li>Shahar Epstein</li>
<li>Shahbaz Aamir</li>
<li>Shoaib UR Rehman</li>
<li>Shubham Raj</li>
<li>Simon Sawicki</li>
<li>Siva Kumar Edupuganti</li>
<li>Sneha Prabhu</li>
<li>Sooter Saalu</li>
<li>Srabasti Banerjee</li>
<li>Stefan Keidel</li>
<li>Steven Loria</li>
<li>Steven Shidi Zhou</li>
<li>Stijn De Haes</li>
<li>Success Moses</li>
<li>TakawaAkirayo</li>
<li>Tamara Janina Fingerlin</li>
<li>Tamas Palinkas</li>
<li>Tatiana Al-Chueyr</li>
<li>Topher Anderson</li>
<li>Tzu-ping Chung</li>
<li>Usiel Riedl</li>
<li>Utkarsh Sharma</li>
<li>Valentyn</li>
<li>Venkat VJ</li>
<li>Vikram Koka</li>
<li>Vikram Medabalimi</li>
<li>Vikramaditya Gaonkar</li>
<li>Vincent</li>
<li>Vincent Kling</li>
<li>VladaZakharova</li>
<li>Waldemar Hummer</li>
<li>Wang Ran (汪然)</li>
<li>Wei Lee</li>
<li>Wojciech Szlachta</li>
<li>Wonseok Yang</li>
<li>Yeonguk Choo</li>
<li>Yohei Kishimoto</li>
<li>Youngha, Park</li>
<li>Yuan Li</li>
<li>Zach Liu</li>
<li>Zhen-Lun (Kevin) Hong</li>
<li>althati</li>
<li>ambikagarg</li>
<li>atrbgithub</li>
<li>awdavidson</li>
<li>codecae</li>
<li>dan-js</li>
<li>darkag</li>
<li>davidfgcorreia</li>
<li>dominikhei</li>
<li>ellisms</li>
<li>enisnazif</li>
<li>fritz-astronomer</li>
<li>gaurav7261</li>
<li>geraj1010</li>
<li>got686-yandex</li>
<li>harjeevan maan</li>
<li>harry.shi</li>
<li>hikaruhk</li>
<li>hprassad</li>
<li>ipsatrivedi</li>
<li>jaejun</li>
<li>jj.lee</li>
<li>jonhspyro</li>
<li>kanagaraj</li>
<li>kandharvishnu</li>
<li>leoguzman</li>
<li>lucasmo</li>
<li>luoyuliuyin</li>
<li>mahdi alizadeh</li>
<li>majorosdonat</li>
<li>max</li>
<li>mayankymailusfedu</li>
<li>michaeljs-c</li>
<li>morooshka</li>
<li>ninad-opsverse</li>
<li>olegkachur-e</li>
<li>paolomoriello</li>
<li>perry2of5</li>
<li>pgvishnuram</li>
<li>phi-friday</li>
<li>rahulgoyal2987</li>
<li>raphaelauv</li>
<li>rgriffier</li>
<li>rom sharon</li>
<li>saucoide</li>
<li>sbock-slack</li>
<li>sc-anssi</li>
<li>seyoon-lim</li>
<li>simonprydden</li>
<li>skandala23</li>
<li>sonu4578</li>
<li>suyesh-amatya</li>
<li>svellaiyan</li>
<li>tnk-ysk</li>
<li>uzhastik</li>
<li>vatsrahul1001</li>
<li>vfeldsher</li>
<li>xavipuerto</li>
<li>xitep</li>
<li>yangyulely</li>
<li>yunchi</li>
<li>鐘翊修</li>
<li>김영준</li>
</ul>
<h2 id="whats-next">What’s Next</h2>
<p>We’d love your feedback. Try out the release, open issues, file PRs, or just join the conversation on the Airflow dev list, Slack, and GitHub.
Let’s build the future of data orchestration—together.</p>

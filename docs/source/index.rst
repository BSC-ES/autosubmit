:html_theme.sidebar_secondary.remove:

.. autosubmit documentation master file, created by
   sphinx-quickstart on Wed Mar 18 16:55:44 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

############################
Autosubmit Workflow Manager
############################

.. toctree::
   :caption: Getting Started
   :maxdepth: 1
   :hidden:

   /qstartguide/index

.. toctree::
   :caption: Installation
   :maxdepth: 1
   :hidden:

   /installation/index

.. toctree::
   :caption: User Guide
   :maxdepth: 1
   :hidden:

   /userguide/index

.. toctree::
   :caption: Developer's Guide
   :maxdepth: 1
   :hidden:

   /devguide/index

.. toctree::
   :caption: Database Documentation
   :maxdepth: 1
   :hidden:

   /database/index

.. toctree::
   :caption: Troubleshooting
   :maxdepth: 1
   :hidden:

   /troubleshooting/index

.. toctree::
   :caption: Module Documentation
   :maxdepth: 1
   :hidden:

   /moduledoc/index

.. toctree::
   :caption: media
   :maxdepth: 1
   :hidden:

   /media/index

.. raw:: html

   <div>
        <div class="row gap-4">
            <div class="col d-flex flex-column justify-content-center">
                <h1 class="fw-bold">Autosubmit</h1>
                <p>
                     Autosubmit is an open source Python <strong>experiment and workflow
                     manager</strong> used to manage complex workflows on Cloud and HPC
                     platforms.
                </p>
                <div class="px-2 py-1 bg-black text-white font-monospace">$ pip install autosubmit</div>
                <div class="mt-3 d-flex gap-2">
                  <a class="btn text-white rounded-pill px-3" style="background-color: #4E8490;" href="qstartguide/index.html">Get started</a>
                  <a class="btn text-white rounded-pill px-3" style="background-color: #4E8490;" href="installation/index.html">Installation</a>
                </div>
            </div>
            <div class="col my-2" style="min-width: 20rem;">
                <img
                  src="_static/isometric.svg"
                  style="background-color: transparent;"
                  alt="Illustration of a person and workflows running on a platform."
                />
            </div>
        </div>
   </div>


Autosubmit is a lightweight workflow manager designed to meet climate research
necessities. Unlike other workflow solutions in the domain, it integrates the
capabilities of an experiment manager, workflow orchestrator and monitor in a
self-contained application.

It is a Python package available at PyPI. The source code in Git contains a
Dockerfile used in cloud environments with Kubernetes, and there are examples
of how to install Autosubmit with Conda.


.. raw:: html

   <div>
      <div class="row g-0 mb-4 pb-4" id="community-logos">
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/bsc.svg" alt="BSC" title="BSC, Barcelona Supercomputing Center" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/upc.svg" alt="UPC" title="UPC, Universitat Politècnica de Catalunya" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/edito-model-lab.png" alt="EDITO" title="EDITO, European Digital Twin Ocean Model Lab" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/destination-earth.svg" alt="DestinE" title="DestinE, Destination Earth" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/copernicus.svg" alt="Copernicus" title="Copernicus Atmospheric Ensemble" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/esiwace.png" alt="ESiWACE" title="ESiWACE, Centre of Excellence in Simulation of Weather and Climate in Europe" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/hanami.png" alt="HANAMI" title="HANAMI" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/eerie.png" alt="EERIE" title="EERIE, European Eddy-Rich Earth System Models" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/specs.png" alt="SPECS" title="SPECS, Seasonal-to-decadal climate Prediction for the Improvement of European Climate Services" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/kit.png" alt="KIT" title="KIT, Karlsruhe Institute of Technology" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/is-enes.png" alt="IS-ENES" title="IS-ENES, Infrastructure for the European Network for Earth System Modelling" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/aq-watch.png" alt="AQ-WATCH" title="AQ-WATCH" />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/cams.png" alt="CAMS" title="CAMS, Copernicus Atmosphere Monitoring Service " />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/bdrc-logo.png" alt="BDRC" title="BDRC, Barcelona Dust Regional Center " />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/dustclim.png" alt="DUSTCLIM" title="DUSTCLIM, Dust Storms Assessment for the development of user-oriented Climate services in Northern Africa, the Middle East and Europe " />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/caliope.png" alt="CALIOPE" title="CALIOPE, CALIdad del aire Operacional Para España " />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/ganana.png" alt="GANANA" title="The Ganana project unites European Union and Indian efforts in scientific High-Performance Computing (HPC) " />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/hpcw.png" alt="HPCW" title="HPCW - The High Performance Climate&Weather Benchmark " />
            </div>
         </div>
         <div class="col-lg-3 col-md-4 col-xs-6">
            <div class="community-logo">
               <img class="img-fluid dark-light" src="_static/logos/terradt.png" alt="TerraDT" title="Digital Twins of Earth System for Cryosphere, Land surface and related interaction " />
            </div>
         </div>
      </div>
      <div class="row gap-4">
         <div class="col" style="min-width: 20rem;">
             <p class="fs-4">
               <i class="fa-solid fa-gear fs-4" style="color: #4E8490;"></i>
               Automation
             </p>
             <p>Management of job submission and dependencies without user intervention.</p>
         </div>
         <div class="col" style="min-width: 20rem;">
             <p class="fs-4">
               <i class="fa-solid fa-fingerprint fs-4" style="color: #4E8490;"></i>
               Data Provenance
             </p>
             <p>Experiments with unique PIDs, use of open standards for data provenance
                in the experiments and workflows.</p>
         </div>
         <div class="col" style="min-width: 20rem;">
             <p class="fs-4">
               <i class="fa-solid fa-clock-rotate-left fs-4" style="color: #4E8490;"></i>
               Fault Tolerance
             </p>
             <p>Automatic retrials and ability to re-run specific parts of
                the experiment in case of failure.</p>
         </div>
         <div class="col" style="min-width: 20rem;">
             <p class="fs-4">
               <i class="fa-solid fa-sitemap fs-4" style="color: #4E8490;"></i>
               Resource Management
             </p>
             <p>Individual platform configuration, allowing users
                to run their experiments without having to modify job scripts.</p>
         </div>
         <div class="col" style="min-width: 20rem;">
             <p class="fs-4">
               <i class="fa-solid fa-server fs-4" style="color: #4E8490;"></i>
               Multiplatform
             </p>
             <p>Widely used to run experiments on different platforms simultaneously, using batch schedulers such as Slurm. It is deployed and used on various HPC and cloud systems.</p>
         </div>
         <div class="col" style="min-width: 20rem;">
             <p class="fs-4">
               <i class="fa-brands fa-github fs-4" style="color: #4E8490;"></i>
               Open Source
             </p>
             <p>Autosubmit code is hosted on GitHub, licensed
                under the GPLv3 License, and under active development.</p>
         </div>
      </div>
   </div>

.. raw:: html

   <div class="mt-4">
      <h4 class="text-center mb-4">More about Autosubmit ecosystem</h4>
      <div class="row gap-4 mb-4 wrap gap-1">
         <div class="col pt-3 border rounded" style="min-width: 20rem;">
             <a class="fs-4 fw-semibold" href="https://autosubmit-api.readthedocs.io/" target="_blank" rel="noreferrer" rel="noopener">
               Autosubmit API
             </a>
             <p class="mt-2">An open-source Python web application that aims to monitor, analyze, and control workflows created and managed with the Autosubmit workflow manager</p>
         </div>
         <div class="col pt-3 border rounded" style="min-width: 20rem;">
             <a class="fs-4 fw-semibold" href="https://autosubmit-gui.readthedocs.io/" target="_blank" rel="noreferrer" rel="noopener">
               Autosubmit GUI
             </a>
             <p class="mt-2">Graphical User Interface that aims to provide users with simplified information from the workflow execution of scientific experiments managed by Autosubmit</p>
         </div>
      </div>
   </div>


Contact Us
==========

.. list-table::
  :widths: 20 80
  :header-rows: 0
  :stub-columns: 1

  * - GitHub
    - https://github.com/BSC-ES/autosubmit
  * - Email
    - support-autosubmit@bsc.es


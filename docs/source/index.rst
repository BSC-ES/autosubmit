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
   :caption: Module Documentation
   :maxdepth: 1
   :hidden:

   /moduledoc/index

.. toctree::
   :caption: Troubleshooting
   :maxdepth: 1
   :hidden:

   /troubleshooting/index

.. toctree::
   :caption: media
   :maxdepth: 1
   :hidden:

   /media/index

.. raw:: html

   <div>
        <div class="row gap-4">
            <div class="col d-flex flex-column justify-content-center">
            <img src="_static/Logo.svg" class="logo__image only-light"/>
            <img src="_static/Logo.svg" class="logo__image only-dark pst-js-only"/>
               <h1 class="visually-hidden">Autosubmit</h1>
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

.. container:: as-lead

   Autosubmit is a lightweight workflow manager designed to meet climate research
   necessities. Unlike other workflow solutions in the domain, it integrates the
   capabilities of an experiment manager, workflow orchestrator and monitor in a
   self-contained application.

   It is a Python package available at PyPI. The source code in Git contains a
   Dockerfile used in cloud environments with Kubernetes, and there are examples
   of how to install Autosubmit with Conda.

What Autosubmit Does
====================

.. grid:: 1 2 3 3
   :gutter: 3
   :class-container: as-features

   .. grid-item-card:: :octicon:`gear;1.1em` Automation
      :class-card: as-card

      Management of job submission and dependencies without user intervention.

   .. grid-item-card:: :octicon:`verified;1.1em` Data Provenance
      :class-card: as-card

      Experiments with unique PIDs, use of open standards for data provenance in
      the experiments and workflows.

   .. grid-item-card:: :octicon:`history;1.1em` Fault Tolerance
      :class-card: as-card

      Automatic retries and ability to re-run specific parts of the experiment in
      case of failure.

   .. grid-item-card:: :octicon:`stack;1.1em` Resource Management
      :class-card: as-card

      Individual platform configuration, allowing users to run their experiments
      without having to modify job scripts.

   .. grid-item-card:: :octicon:`server;1.1em` Multiplatform
      :class-card: as-card

      Widely used to run experiments on different platforms simultaneously, using
      batch schedulers such as Slurm. It is deployed and used on various HPC and
      cloud systems.

   .. grid-item-card:: :octicon:`mark-github;1.1em` Open Source
      :class-card: as-card

      Autosubmit code is hosted on GitHub, licensed under the GPLv3 License, and
      under active development.

Where Autosubmit Is Used
========================

Autosubmit runs the workflows behind these research centres, projects and
operational services.

.. raw:: html

   <div class="row g-0 mb-4 pb-4" id="community-logos">
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/bsc.svg" alt="BSC" title="BSC, Barcelona Supercomputing Center" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/upc.svg" alt="UPC" title="UPC, Universitat Politècnica de Catalunya" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/edito-model-lab.png" alt="EDITO" title="EDITO, European Digital Twin Ocean Model Lab" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/destination-earth.svg" alt="DestinE" title="DestinE, Destination Earth" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/copernicus.svg" alt="Copernicus" title="Copernicus Atmospheric Ensemble" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/esiwace.png" alt="ESiWACE" title="ESiWACE, Centre of Excellence in Simulation of Weather and Climate in Europe" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/hanami.png" alt="HANAMI" title="HANAMI" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/eerie.png" alt="EERIE" title="EERIE, European Eddy-Rich Earth System Models" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/specs.png" alt="SPECS" title="SPECS, Seasonal-to-decadal climate Prediction for the Improvement of European Climate Services" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/kit.png" alt="KIT" title="KIT, Karlsruhe Institute of Technology" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/is-enes.png" alt="IS-ENES" title="IS-ENES, Infrastructure for the European Network for Earth System Modelling" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/aq-watch.png" alt="AQ-WATCH" title="AQ-WATCH" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/cams.png" alt="CAMS" title="CAMS, Copernicus Atmosphere Monitoring Service" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/bdrc-logo.png" alt="BDRC" title="BDRC, Barcelona Dust Regional Center" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/dustclim.png" alt="DUSTCLIM" title="DUSTCLIM, Dust Storms Assessment for the development of user-oriented Climate services in Northern Africa, the Middle East and Europe" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/caliope.png" alt="CALIOPE" title="CALIOPE, CALIdad del aire Operacional Para España" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/ganana.png" alt="GANANA" title="GANANA, uniting European Union and Indian efforts in scientific High-Performance Computing" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/hpcw.png" alt="HPCW" title="HPCW, The High Performance Climate and Weather Benchmark" /></div></div>
      <div class="col-lg-3 col-md-4 col-xs-6"><div class="community-logo"><img class="img-fluid dark-light" src="_static/logos/terradt.png" alt="TerraDT" title="TerraDT, Digital Twins of Earth System for Cryosphere, Land surface and related interaction" /></div></div>
   </div>

The Autosubmit Ecosystem
========================

Two companion tools extend Autosubmit with monitoring and a graphical interface.
Both are open source and documented separately.

.. grid:: 1 1 2 2
   :gutter: 3
   :class-container: as-eco

   .. grid-item-card:: Autosubmit API
      :img-top: media/fig/as_api.svg
      :class-img-top: dark-light
      :link: https://autosubmit-api.readthedocs.io/
      :class-card: as-card

      A Python web application that monitors, analyses and controls workflows
      created and managed with Autosubmit. It exposes experiment state over HTTP
      and is what the GUI reads from.

   .. grid-item-card:: Autosubmit GUI
      :img-top: media/fig/as_gui.svg
      :class-img-top: dark-light
      :link: https://autosubmit-gui.readthedocs.io/
      :class-card: as-card
      
      A graphical interface that presents workflow execution in the browser, with
      job trees, run history and log inspection for experiments managed by
      Autosubmit.

Citing Autosubmit
=================

.. container:: as-citation

   .. citation::

Contact Us
==========

.. grid:: 1 3 3 3
   :gutter: 3
   :class-container: as-contact

   .. grid-item-card:: :octicon:`repo;1.1em` Source code
      :link: https://github.com/BSC-ES/autosubmit
      :class-card: as-card

      Browse the code, read the changelog, or open a pull request.

      +++
      BSC-ES/autosubmit

   .. grid-item-card:: :octicon:`issue-opened;1.1em` Report a problem
      :link: https://github.com/BSC-ES/autosubmit/issues
      :class-card: as-card

      Found a bug or want to request a feature? Open an issue on the tracker.

      +++
      Issue tracker

   .. grid-item-card:: :octicon:`mail;1.1em` Ask the team
      :link: mailto:support-autosubmit@bsc.es
      :class-card: as-card

      Questions about running Autosubmit at your centre, or anything the docs do
      not answer.

      +++
      support-autosubmit@bsc.es

Media and Press Kit
===================

.. container:: as-media

   Logos in SVG and PNG, and slide decks — everything you need to include
   Autosubmit in your own materials.

   :doc:`Open the media kit </media/index>`
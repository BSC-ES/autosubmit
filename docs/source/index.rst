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
                <p>Widely used to run experiments on different platforms simultaneously, using batch schedulers such as Slurm, PBS, LSF. It is deployed and used on various HPC and cloud systems.</p>
            </div>
            <div class="col" style="min-width: 20rem;">
                <p class="fs-4">
                  <i class="fa-brands fa-gitlab fs-4" style="color: #4E8490;"></i>
                  Open Source
                </p>
                <p>Autosubmit code is hosted at BSC Earth Sciences' GitLab, licensed
                   under the GPLv3 License, and under active development.</p>
            </div>
        </div>
    </div>

Contact Us
==========

.. list-table::
  :widths: 20 80
  :header-rows: 0
  :stub-columns: 1

  * - GitLab
    - https://earth.bsc.es/gitlab/es/autosubmit/
  * - Email
    - support-autosubmit@bsc.es


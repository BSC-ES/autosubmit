##########
Provenance
##########

Autosubmit manages experiments following the `FAIR data`_ principles,
findability, accessibility, interoperability, and reusability. It
supports and uses open standards such as YAML, RO-Crate, as well as
other standards such as ISO-8601.

Each Autosubmit experiment is assigned a :doc:`unique experiment ID <expids>`
(also called expid). It also provides a central database and utilities
that permit experiments to be referenced.

The commands issued by users generate log files, and become part of the
Autosubmit experiment, as explained in the :doc:`Traceability section <traceability>`.

Users can :ref:`archive Autosubmit experiments <archive>`. These archives contain the complete
logs and other files in the experiment directory, and can be later unarchived
and executed again. Supported archival formats are ZIP and **RO-Crate**.

RO-Crate
--------

RO-Crate is a community standard adopted by other workflow managers
to package research data with their metadata. It is extensible, and contains
profiles to package computational workflows. From the `RO-Crate`_ website,
“What is RO-Crate?”:

.. pull-quote::
  RO-Crate is a community effort to establish a lightweight approach to
  packaging research data with their metadata. It is based on schema.org
  annotations in JSON-LD, and aims to make best-practice in formal
  metadata description accessible and practical for use in a wider variety
  of situations, from an individual researcher working with a folder of
  data, to large data-intensive computational research environments.

Autosubmit `conforms`_ to the following RO-Crate profiles:

* Process Run Crate

* Workflow Run Crate

* Workflow RO-Crate

Experiments archived as RO-Crate can also be uploaded to `Zenodo`_ and
to `WorkflowHub`_. The Autosubmit team worked with the WorkflowHub team
to add Autosubmit as a supported language for workflows. Both Zenodo
and WorkflowHub are issuers of `DOI`_'s (digital object identifiers),
which can be used as persistent identifiers to resolve Autosubmit
experiments referenced in papers and other documents.

.. _FAIR data: https://en.wikipedia.org/wiki/FAIR_data

.. _RO-Crate: https://www.researchobject.org/ro-crate/

.. _conforms: https://github.com/ResearchObject/workflow-run-crate/pull/61

.. _Zenodo: https://zenodo.org/

.. _WorkflowHub: https://workflowhub.eu/

.. _DOI: https://en.wikipedia.org/wiki/Digital_object_identifier

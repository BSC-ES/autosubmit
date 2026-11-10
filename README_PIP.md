Autosubmit is a lightweight workflow manager and meta-scheduler. It was originally designed in 2011 for use in climate research for configuring and running experiments. In the last few years, it has been extended to cover additional use cases, and today it is used as a general orchestration tool. It supports scheduling jobs to remote batch servers (via SSH) such as PBS, LSF, SLURM, and SGE.

Autosubmit is a Python package provided in PyPI, which facilitates easy and fast integration and relocation on new platforms. Conda recipes are available on the project website. A containerized version for testing purposes is also available but not public yet.

The features found in Autosubmit characterize it as both an experiment manager and also as a workflow orchestrator. The experiment manager allows users to define and configure experiments, supported by a hierarchical database that ensures reproducibility and traceability. The orchestrator is designed to run complex workflows in research and operational mode by managing their dependencies and interfacing with local and remote hosts.

These multi-scale workflows can contain from a few steps to thousands of steps, and from a single platform to multiple platforms. Platform is a concept in Autosubmit to abstract servers. A workflow configuration can include one or multiple platforms, allowing the workflow to run on any number of servers via password-less SSH without any external deployment.

Due to its robustness it can handle different eventualities such as networking connectivity issues or I/O errors. The monitoring capabilities extend beyond the command-line application through a REST API that allows communication with workflow monitoring tools such as the Autosubmit web GUI.

It has contributed to various European research projects and runs different operational systems. It will support the Earth Digital Twins as the Digital Twin Ocean over the next years.

It is currently used at the Barcelona Supercomputing Centre (BSC) to run models (EC-Earth, MONARCH, NEMO, CALIOPE, HERMES, and others), operational toolchains (S2S4E), data-download workflows (ECMWF MARS), and for many other use cases. Autosubmit has been used to run workflows in different supercomputers at BSC, ECMWF, IC3, CESGA, EPCC, PDC, and OLCF.


def test_auto_splits_dependency_creates(autosubmit_exp):
    config =  {
            "CONFIG": {"SAFETYSLEEPTIME": 0, "TOTALJOBS": 20, "MAXWAITINGJOBS": 20},
            "DEFAULT": {"HPCARCH": "local"},
            "EXPERIMENT": {
                "DATELIST": "20200101",
                "MEMBERS": "fc0",
                "CHUNKSIZEUNIT": "month",
                "SPLITSIZEUNIT": "day",
                "CHUNKSIZE": 1,
                "NUMCHUNKS": 2,
                "CALENDAR": "standard",
            },
            "JOBS": {
                "CHILD": {
                    "SCRIPT": "echo ok",
                    "RUNNING": "chunk",
                    "SPLITS": "auto",
                    "DEPENDENCIES": {
                        "CHILD": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}},
                        "CHILD-1": {},
                    },
                },
            },
        }
    as_exp = autosubmit_exp(experiment_data=config, include_jobs=False, create=True)

    job_list = as_exp.autosubmit.load_job_list(
        as_exp.expid, as_exp.as_conf, new=False, full_load=True,
    )
    jobs = [j for j in job_list.job_list if j.name.startswith(as_exp.expid)]
    edges_ours = [e for e in job_list.graph.edges if e[0].startswith(as_exp.expid)]
    assert len(jobs) == 60
    assert len(edges_ours) == len(jobs) - 1
